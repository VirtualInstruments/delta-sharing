/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sharing.server.telemetry

import java.util.UUID
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import io.delta.standalone.DeltaLog
import io.delta.standalone.Operation
import io.delta.standalone.actions.AddFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{MessageType, Types => PTypes}
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, BOOLEAN, INT64}
import org.slf4j.LoggerFactory

/**
 * Writes ACCESS_LOG entries asynchronously to a single consolidated Delta table on GCS.
 *
 * Records are buffered in a bounded in-memory queue and written by a single background
 * daemon thread. The calling thread is never blocked: records are silently dropped when
 * the queue is full. Write failures are logged to stderr and never propagate to callers.
 *
 * All access logs are written to a single table at `{basePath}/access_log_br__system`,
 * with tenant_id included as a field for filtering. The table is not partitioned to
 * simplify queries across all tenants.
 *
 * IMPORTANT: The Delta table must be pre-created by the deltalake-admin tool during
 * tenant onboarding. This writer does not create the table schema.
 *
 * Only ACCESS_LOG entries are written; PRICING_CONTEXT and REQUEST_HEADERS are ignored.
 *
 * @param basePath GCS base path for the consolidated access log table
 *        (e.g. gs://bucket/datalake/data/tenant/_system)
 * @param flushIntervalSeconds how often to flush buffered records (seconds)
 * @param flushBatchSize maximum records per flush (triggers early flush when reached)
 */
class DeltaAccessLogWriter(
    basePath: String,
    flushIntervalSeconds: Int,
    flushBatchSize: Int) extends AccessLogEmitter {

  private val logger = LoggerFactory.getLogger(classOf[DeltaAccessLogWriter])

  private val MaxQueueCapacity = 100000
  private val queue = new LinkedBlockingQueue[AccessLogEntry](MaxQueueCapacity)
  private val stopped = new AtomicBoolean(false)
  private val flushIntervalMs = flushIntervalSeconds.toLong * 1000L

  // Reuse a single Configuration; GCS credentials come from Workload Identity or
  // GOOGLE_APPLICATION_CREDENTIALS, picked up automatically by the GCS Hadoop connector.
  private val conf = withClassLoader(new Configuration())

  // Parquet schema: all data columns including audit fields (no partitioning).
  private val parquetSchema: MessageType = new MessageType("access_log",
    PTypes.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("logType"),
    PTypes.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("share"),
    PTypes.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("schema"),
    PTypes.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("table"),
    PTypes.required(INT64).named("egressBytes"),
    PTypes.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("pricingTier"),
    PTypes.required(INT64).named("timestampMs"),
    PTypes.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("requestType"),
    PTypes.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("clientRegion"),
    // Audit fields for customer audits and consolidated storage
    PTypes.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("tenantId"),
    PTypes.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("clientIp"),
    PTypes.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("rawRegionHeader"),
    PTypes.optional(BOOLEAN).named("isGcpIp")
  )

  // How often the flush loop wakes to check the stopped flag and time-based flush.
  // Short enough for responsive shutdown; long enough to avoid busy-waiting.
  private val CheckIntervalMs = 500L

  private val flushThread: Thread = {
    val t = new Thread(() => runFlushLoop(), "delta-access-log-writer")
    t.setDaemon(true)
    t.start()
    t
  }

  override def record(entry: AccessLogEntry): Unit = {
    if (entry.egressBytes <= 0) return
    if (!queue.offer(entry)) {
      logger.warn(
        "Delta access log queue is full ({} capacity); " +
          "dropping record for tenant {} share {}/{}/{}",
        MaxQueueCapacity: Integer,
        extractTenantId(entry.share),
        entry.share,
        entry.schema,
        entry.table
      )
    }
  }

  override def recordContext(entry: PricingContextLogEntry): Unit = {}
  override def recordHeaders(entry: RequestHeadersLogEntry): Unit = {}

  override def close(): Unit = {
    stopped.set(true)
    flushThread.join(30000L)
  }

  private def runFlushLoop(): Unit = {
    val batch = new java.util.ArrayList[AccessLogEntry]()
    var lastFlushMs = System.currentTimeMillis()

    while (!stopped.get()) {
      try {
        val head = queue.poll(CheckIntervalMs, TimeUnit.MILLISECONDS)
        if (head != null) {
          batch.add(head)
          queue.drainTo(batch, flushBatchSize - 1)
        }

        val elapsed = System.currentTimeMillis() - lastFlushMs
        val timeToFlush = elapsed >= flushIntervalMs
        val batchFull = batch.size() >= flushBatchSize
        if ((timeToFlush || batchFull) && batch.size() > 0) {
          safeWriteBatch(batch.asScala.toList)
          batch.clear()
          lastFlushMs = System.currentTimeMillis()
        }
      } catch {
        case _: InterruptedException => // ignore; re-check stopped on next iteration
        case e: Exception =>
          logger.error("Unexpected error in delta access log flush loop", e)
      }
    }

    // Final flush: drain any remaining records before shutdown.
    // Respect batch size to maintain consistent behavior (important for tests and
    // scenarios where each record should produce a separate file).
    queue.drainTo(batch)
    if (!batch.isEmpty) {
      val entries = batch.asScala.toList
      entries.grouped(flushBatchSize).foreach(safeWriteBatch)
    }
  }

  private def safeWriteBatch(entries: List[AccessLogEntry]): Unit = {
    try {
      withClassLoader(writeBatch(entries))
    } catch {
      case e: Exception =>
        logger.error(s"Failed to write ${entries.size} access log entries to Delta table", e)
    }
  }

  /**
   * Extracts tenant_id from share name following the pattern `{tenant_id}_share`.
   * Falls back to the share name itself if the pattern doesn't match.
   */
  private def extractTenantId(shareName: String): String = {
    if (shareName.endsWith("_share")) {
      shareName.dropRight("_share".length)
    } else {
      // Fallback for shares not following the convention
      shareName
    }
  }

  /**
   * Returns the path to the consolidated access log table.
   * Table is named access_log_br__system (double underscore because _system tenant starts with _)
   */
  private def consolidatedTablePath: String = {
    val normalizedBase = if (basePath.endsWith("/")) basePath.dropRight(1) else basePath
    s"$normalizedBase/access_log_br__system"
  }

  private def writeBatch(entries: List[AccessLogEntry]): Unit = {
    // Write all entries to the single consolidated table
    val tablePath = consolidatedTablePath

    val deltaLog = DeltaLog.forTable(conf, new Path(tablePath))
    val txn = deltaLog.startTransaction()

    // Table must be pre-created by deltalake-admin tool
    if (txn.readVersion() < 0) {
      logger.error(
        s"Delta table does not exist at $tablePath. " +
        "Table must be created by deltalake-admin during tenant onboarding.")
      return
    }

    // Enrich entries with tenantId if not already set
    val enrichedEntries = entries.map { e =>
      if (e.tenantId.isEmpty) {
        e.copy(tenantId = Some(extractTenantId(e.share)))
      } else {
        e
      }
    }

    val addFile: Option[io.delta.standalone.actions.Action] =
      writeParquetFile(tablePath, enrichedEntries)

    val addFiles: Seq[io.delta.standalone.actions.Action] = addFile.toSeq

    if (addFiles.nonEmpty) {
      val operation = new Operation(Operation.Name.WRITE)
      txn.commit(addFiles.asJava, operation, "delta-sharing-server")
    }
  }

  private def writeParquetFile(
      tablePath: String,
      entries: List[AccessLogEntry]): Option[io.delta.standalone.actions.Action] = {
    val relPath = s"${UUID.randomUUID()}.parquet"
    val absPath = new Path(s"$tablePath/$relPath")
    try {
      val factory = new SimpleGroupFactory(parquetSchema)
      val parquetConf = new Configuration(conf)
      val writer = ExampleParquetWriter.builder(absPath)
        .withType(parquetSchema)
        .withConf(parquetConf)
        .withWriteMode(ParquetFileWriter.Mode.CREATE)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()
      try {
        for (e <- entries) {
          val g = factory.newGroup()
          g.add("logType", Binary.fromString("ACCESS_LOG"))
          g.add("share", Binary.fromString(e.share))
          g.add("schema", Binary.fromString(e.schema))
          g.add("table", Binary.fromString(e.table))
          g.add("egressBytes", e.egressBytes)
          g.add("pricingTier", Binary.fromString(e.pricingTier))
          g.add("timestampMs", e.timestampMs)
          g.add("requestType", Binary.fromString(e.requestType))
          e.clientRegion.foreach(r => g.add("clientRegion", Binary.fromString(r)))
          // Audit fields
          e.tenantId.foreach(t => g.add("tenantId", Binary.fromString(t)))
          e.clientIp.foreach(ip => g.add("clientIp", Binary.fromString(ip)))
          e.rawRegionHeader.foreach(h => g.add("rawRegionHeader", Binary.fromString(h)))
          e.isGcpIp.foreach(b => g.add("isGcpIp", b))
          writer.write(g)
        }
      } finally {
        writer.close()
      }

      val fs = absPath.getFileSystem(conf)
      val fileSize = fs.getFileStatus(absPath).getLen
      // No partition values - empty map
      Some(AddFile.builder(relPath, java.util.Collections.emptyMap[String, String](),
        fileSize, System.currentTimeMillis(), true).build())
    } catch {
      case e: Exception =>
        logger.error(s"Failed to write parquet file $relPath", e)
        None
    }
  }

  private def withClassLoader[T](func: => T): T = {
    val classLoader = Thread.currentThread().getContextClassLoader
    if (classLoader == null) {
      Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
      try func finally Thread.currentThread().setContextClassLoader(null)
    } else {
      func
    }
  }
}
