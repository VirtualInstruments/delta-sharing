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
import io.delta.standalone.actions.{AddFile, Format, Metadata, Protocol}
import io.delta.standalone.types.{IntegerType, LongType, StringType, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{MessageType, Types => PTypes}
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.{BINARY, INT64}
import org.slf4j.LoggerFactory

/**
 * Writes ACCESS_LOG entries asynchronously to per-tenant Delta tables on GCS.
 *
 * Records are buffered in a bounded in-memory queue and written by a single background
 * daemon thread. The calling thread is never blocked: records are silently dropped when
 * the queue is full. Write failures are logged to stderr and never propagate to callers.
 *
 * Access logs are split into per-tenant tables based on the share name. The tenant_id is
 * extracted from share names following the pattern `{tenant_id}_share`. Each tenant gets
 * its own table at `{basePath}/access_log_{tenant_id}`.
 *
 * The Delta tables are auto-created on the first write if they do not already exist.
 * Only ACCESS_LOG entries are written; PRICING_CONTEXT and REQUEST_HEADERS are ignored.
 *
 * @param basePath GCS base path for tenant access log tables
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

  // Parquet schema: data columns stored in the file body (partition cols excluded).
  private val parquetSchema: MessageType = new MessageType("access_log",
    PTypes.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("logType"),
    PTypes.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("share"),
    PTypes.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("schema"),
    PTypes.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("table"),
    PTypes.required(INT64).named("egressBytes"),
    PTypes.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("pricingTier"),
    PTypes.required(INT64).named("timestampMs"),
    PTypes.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("requestType"),
    PTypes.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("clientRegion")
  )

  // Delta schema: data columns + partition columns.
  private val deltaSchema: StructType = new StructType()
    .add("logType", new StringType(), false)
    .add("share", new StringType(), false)
    .add("schema", new StringType(), false)
    .add("table", new StringType(), false)
    .add("egressBytes", new LongType(), false)
    .add("pricingTier", new StringType(), true)
    .add("timestampMs", new LongType(), false)
    .add("requestType", new StringType(), true)
    .add("clientRegion", new StringType(), true)
    .add("year", new IntegerType(), false)
    .add("month", new IntegerType(), false)
    .add("day", new IntegerType(), false)

  private val partitionCols: java.util.List[String] = List("year", "month", "day").asJava

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
        "Delta access log queue is full ({} capacity); dropping record for {}/{}/{}",
        MaxQueueCapacity: Integer,
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
    queue.drainTo(batch)
    if (!batch.isEmpty) {
      safeWriteBatch(batch.asScala.toList)
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
   * Falls back to `_unknown` if the pattern doesn't match.
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
   * Constructs the full table path for a given tenant_id.
   */
  private def tablePathForTenant(tenantId: String): String = {
    val normalizedBase = if (basePath.endsWith("/")) basePath.dropRight(1) else basePath
    s"$normalizedBase/access_log_$tenantId"
  }

  private def writeBatch(entries: List[AccessLogEntry]): Unit = {
    // Group by tenant_id first, then by date partition within each tenant
    val groupedByTenant = entries.groupBy(e => extractTenantId(e.share))

    for ((tenantId, tenantEntries) <- groupedByTenant) {
      writeTenantBatch(tenantId, tenantEntries)
    }
  }

  private def writeTenantBatch(tenantId: String, entries: List[AccessLogEntry]): Unit = {
    val tablePath = tablePathForTenant(tenantId)
    val grouped = entries.groupBy { e =>
      val cal = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
      cal.setTimeInMillis(e.timestampMs)
      (cal.get(java.util.Calendar.YEAR),
        cal.get(java.util.Calendar.MONTH) + 1,
        cal.get(java.util.Calendar.DAY_OF_MONTH))
    }

    val deltaLog = DeltaLog.forTable(conf, new Path(tablePath))
    val txn = deltaLog.startTransaction()
    val isNewTable = txn.readVersion() < 0

    if (isNewTable) {
      val metadata = Metadata.builder()
        .schema(deltaSchema)
        .format(new Format())
        .partitionColumns(partitionCols)
        .configuration(java.util.Collections.emptyMap[String, String]())
        .createdTime(System.currentTimeMillis())
        .build()
      txn.updateMetadata(metadata)
    }

    val addFiles: Seq[io.delta.standalone.actions.Action] =
      grouped.flatMap { case ((year, month, day), partEntries) =>
        writeParquetPartition(tablePath, year, month, day, partEntries)
      }.toSeq

    val allActions: Seq[io.delta.standalone.actions.Action] =
      if (isNewTable) new Protocol(1, 2) +: addFiles else addFiles

    val operation = if (isNewTable) {
      new Operation(Operation.Name.CREATE_TABLE)
    } else {
      new Operation(Operation.Name.WRITE)
    }

    txn.commit(allActions.asJava, operation, "delta-sharing-server")
  }

  private def writeParquetPartition(
      tablePath: String,
      year: Int,
      month: Int,
      day: Int,
      entries: List[AccessLogEntry]): Option[io.delta.standalone.actions.Action] = {
    val relPath = s"year=$year/month=$month/day=$day/${UUID.randomUUID()}.parquet"
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
          writer.write(g)
        }
      } finally {
        writer.close()
      }

      val fs = absPath.getFileSystem(conf)
      val fileSize = fs.getFileStatus(absPath).getLen
      val partVals = Map(
        "year" -> year.toString,
        "month" -> month.toString,
        "day" -> day.toString)
      Some(AddFile.builder(relPath, partVals.asJava, fileSize, System.currentTimeMillis(), true)
        .build())
    } catch {
      case e: Exception =>
        logger.error(s"Failed to write parquet file for year=$year/month=$month/day=$day", e)
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
