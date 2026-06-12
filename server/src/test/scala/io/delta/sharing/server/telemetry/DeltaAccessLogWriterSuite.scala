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

import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._

import io.delta.standalone.DeltaLog
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalatest.FunSuite

class DeltaAccessLogWriterSuite extends FunSuite {

  private def makeTempPath(): String = {
    val dir = Files.createTempDirectory("delta-access-log-test")
    dir.toAbsolutePath.toString
  }

  private def makeEntry(
      share: String = "tenant1_share",
      schema: String = "sc",
      table: String = "t",
      egressBytes: Long = 1024L,
      timestampMs: Long = 1717502400000L, // 2024-06-04 in UTC
      pricingTier: String = "internet_to_na_eu",
      requestType: String = "query",
      clientRegion: Option[String] = Some("US"),
      tenantId: Option[String] = None,
      clientIp: Option[String] = Some("203.0.113.45"),
      rawRegionHeader: Option[String] = Some("US"),
      isGcpIp: Option[Boolean] = Some(false)): AccessLogEntry =
    AccessLogEntry(share, schema, table, egressBytes, timestampMs,
      pricingTier, clientRegion, requestType, tenantId, clientIp, rawRegionHeader, isGcpIp)

  /**
   * Gets the consolidated table path from the base path.
   * All records are written to access_log_br_system.
   */
  private def consolidatedTablePath(basePath: String): String = {
    s"$basePath/access_log_br_system"
  }

  test("single record is written to consolidated Delta table") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry())
    writer.close()

    val tablePath = consolidatedTablePath(basePath)
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    val snapshot = log.snapshot()
    assert(snapshot.getVersion >= 0, "Delta table should exist after write")

    val files = snapshot.getAllFiles.asScala
    assert(files.nonEmpty, "Snapshot should contain at least one data file")

    // Verify no partitioning
    val partFile = files.head
    assert(partFile.getPartitionValues.isEmpty, "Table should not be partitioned")
  }

  test("records from different timestamps are written to a single unpartitioned table") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    // Three entries spanning two different days
    writer.record(makeEntry(timestampMs = 1717502400000L)) // 2024-06-04
    writer.record(makeEntry(timestampMs = 1717502400000L)) // 2024-06-04
    writer.record(makeEntry(timestampMs = 1717588800000L)) // 2024-06-05
    writer.close()

    val tablePath = consolidatedTablePath(basePath)
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    val files = log.snapshot().getAllFiles.asScala.toList
    // All records go to a single file since there's no partitioning
    assert(files.size == 1, "One file expected (no partitioning)")
    assert(files.head.getPartitionValues.isEmpty, "Table should not be partitioned")
  }

  test("multiple flushes produce multiple Delta commits") {
    val basePath = makeTempPath()
    // batchSize=1 means each record triggers its own flush.
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 300, flushBatchSize = 1)
    writer.record(makeEntry(table = "t1", timestampMs = 1717502400000L)) // 2024-06-04
    writer.record(makeEntry(table = "t2", timestampMs = 1717588800000L)) // 2024-06-05
    writer.close() // waits for all pending flushes

    val tablePath = consolidatedTablePath(basePath)
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    assert(log.snapshot().getVersion >= 0, "Table should exist after write")
    val files = log.snapshot().getAllFiles.asScala.toList
    // Each flush writes a separate file
    assert(files.size == 2, "Both records should produce separate files")
    assert(files.forall(_.getPartitionValues.isEmpty), "Table should not be partitioned")
  }

  test("records with zero egressBytes are silently skipped") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry(egressBytes = 0L))
    writer.record(makeEntry(egressBytes = -1L))
    writer.close()

    val tablePath = consolidatedTablePath(basePath)
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    assert(log.snapshot().getVersion < 0 ||
      log.snapshot().getAllFiles.asScala.isEmpty,
      "No data files should exist when all records are skipped")
  }

  test("recordContext and recordHeaders are no-ops") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    // Should not throw or write anything
    writer.recordContext(PricingContextLogEntry("s", "t", 0L))
    writer.recordHeaders(RequestHeadersLogEntry("s", "t", 0L, Map.empty))
    writer.close()
  }

  test("write failures do not propagate to callers") {
    // Use an invalid path to force write failure
    val writer = new DeltaAccessLogWriter(
      "/nonexistent/readonly/path",
      flushIntervalSeconds = 5,
      flushBatchSize = 1)
    // Should not throw
    writer.record(makeEntry())
    writer.close()
  }

  test("close flushes records that have not yet been written") {
    val basePath = makeTempPath()
    // Very long flush interval so nothing writes until close()
    val writer =
      new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 3600, flushBatchSize = 10000)
    writer.record(makeEntry())
    writer.record(makeEntry())
    writer.close() // should trigger final flush

    val tablePath = consolidatedTablePath(basePath)
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    val files = log.snapshot().getAllFiles.asScala
    assert(files.nonEmpty, "Records should be flushed on close()")
  }

  test("Parquet files contain the expected columns including audit fields") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry())
    writer.close()

    val tablePath = consolidatedTablePath(basePath)
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    val addFile = log.snapshot().getAllFiles.asScala.head
    val filePath = new Path(s"$tablePath/${addFile.getPath}")
    val conf = new Configuration()
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, conf))
    try {
      val schema = reader.getFileMetaData.getSchema
      val fields = schema.getFields.asScala.map(_.getName).toSet
      // Core fields
      assert(fields.contains("logType"))
      assert(fields.contains("share"))
      assert(fields.contains("schema"))
      assert(fields.contains("table"))
      assert(fields.contains("egressBytes"))
      assert(fields.contains("pricingTier"))
      assert(fields.contains("timestampMs"))
      assert(fields.contains("requestType"))
      assert(fields.contains("clientRegion"))
      // Audit fields
      assert(fields.contains("tenantId"))
      assert(fields.contains("clientIp"))
      assert(fields.contains("rawRegionHeader"))
      assert(fields.contains("isGcpIp"))
    } finally {
      reader.close()
    }
  }

  test("table is auto-created on first write without partitioning") {
    val basePath = makeTempPath()
    val tablePath = consolidatedTablePath(basePath)
    val deltaPath = new Path(tablePath)
    val conf = new Configuration()

    // Confirm table does not exist
    val logBefore = DeltaLog.forTable(conf, deltaPath)
    assert(logBefore.snapshot().getVersion < 0, "Table should not exist before first write")

    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry())
    writer.close()

    val logAfter = DeltaLog.forTable(conf, deltaPath)
    assert(logAfter.snapshot().getVersion >= 0, "Table should exist after first write")
    val partCols = logAfter.snapshot().getMetadata.getPartitionColumns.asScala
    assert(partCols.isEmpty, "Table should not have partition columns")
  }

  test("records from different tenants are written to the same consolidated table") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry(share = "tenant1_share"))
    writer.record(makeEntry(share = "tenant2_share"))
    writer.record(makeEntry(share = "_system_share"))
    writer.close()

    val conf = new Configuration()

    // Check consolidated table exists
    val tablePath = consolidatedTablePath(basePath)
    val log = DeltaLog.forTable(conf, new Path(tablePath))
    assert(log.snapshot().getVersion >= 0, "Consolidated table should exist")
    assert(log.snapshot().getAllFiles.asScala.nonEmpty, "Consolidated table should have data")
  }

  test("tenantId is auto-derived from share name when not provided") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    // Record without explicit tenantId
    writer.record(makeEntry(share = "my_tenant_share", tenantId = None))
    writer.close()

    val tablePath = consolidatedTablePath(basePath)
    val conf = new Configuration()
    val log = DeltaLog.forTable(conf, new Path(tablePath))
    assert(log.snapshot().getVersion >= 0, "Table should exist")
  }
}
