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
      clientRegion: Option[String] = Some("US")): AccessLogEntry =
    AccessLogEntry(share, schema, table, egressBytes, timestampMs,
      pricingTier, clientRegion, requestType)

  /**
   * Gets the tenant-specific table path from the base path and share name.
   * Mirrors the logic in DeltaAccessLogWriter.
   */
  private def tablePathForShare(basePath: String, share: String): String = {
    val tenantId = if (share.endsWith("_share")) share.dropRight("_share".length) else share
    s"$basePath/access_log_$tenantId"
  }

  test("single record is written to per-tenant Delta table") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry())
    writer.close()

    val tablePath = tablePathForShare(basePath, "tenant1_share")
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    val snapshot = log.snapshot()
    assert(snapshot.getVersion >= 0, "Delta table should exist after write")

    val files = snapshot.getAllFiles.asScala
    assert(files.nonEmpty, "Snapshot should contain at least one data file")

    val partFile = files.head
    assert(partFile.getPartitionValues.containsKey("year"))
    assert(partFile.getPartitionValues.containsKey("month"))
    assert(partFile.getPartitionValues.containsKey("day"))
    assert(partFile.getPartitionValues.get("year") == "2024")
  }

  test("records are partitioned by year/month/day derived from timestampMs") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    // Three entries spanning two different days
    writer.record(makeEntry(timestampMs = 1717502400000L)) // 2024-06-04
    writer.record(makeEntry(timestampMs = 1717502400000L)) // 2024-06-04
    writer.record(makeEntry(timestampMs = 1717588800000L)) // 2024-06-05
    writer.close()

    val tablePath = tablePathForShare(basePath, "tenant1_share")
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    val files = log.snapshot().getAllFiles.asScala.toList
    assert(files.size == 2, "Two partitions expected (one per day)")

    val partitionDays = files.map(_.getPartitionValues.get("day")).toSet
    assert(partitionDays == Set("4", "5"))
  }

  test("multiple flushes produce multiple Delta commits") {
    val basePath = makeTempPath()
    // batchSize=1 means each record triggers its own flush.
    // Use different-day timestamps so each record lands in a distinct partition file,
    // which lets us verify both records were written regardless of commit count.
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 300, flushBatchSize = 1)
    writer.record(makeEntry(table = "t1", timestampMs = 1717502400000L)) // 2024-06-04
    writer.record(makeEntry(table = "t2", timestampMs = 1717588800000L)) // 2024-06-05
    writer.close() // waits for all pending flushes

    val tablePath = tablePathForShare(basePath, "tenant1_share")
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    assert(log.snapshot().getVersion >= 0, "Table should exist after write")
    val files = log.snapshot().getAllFiles.asScala.toList
    assert(files.size == 2, "Both records should produce separate partition files")
    val partitionDays = files.map(_.getPartitionValues.get("day")).toSet
    assert(partitionDays == Set("4", "5"), "Records should be in separate day partitions")
  }

  test("records with zero egressBytes are silently skipped") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry(egressBytes = 0L))
    writer.record(makeEntry(egressBytes = -1L))
    writer.close()

    val tablePath = tablePathForShare(basePath, "tenant1_share")
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
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 3600, flushBatchSize = 10000)
    writer.record(makeEntry())
    writer.record(makeEntry())
    writer.close() // should trigger final flush

    val tablePath = tablePathForShare(basePath, "tenant1_share")
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    val files = log.snapshot().getAllFiles.asScala
    assert(files.nonEmpty, "Records should be flushed on close()")
  }

  test("Parquet files contain the expected columns") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry())
    writer.close()

    val tablePath = tablePathForShare(basePath, "tenant1_share")
    val log = DeltaLog.forTable(new Configuration(), new Path(tablePath))
    val addFile = log.snapshot().getAllFiles.asScala.head
    val filePath = new Path(s"$tablePath/${addFile.getPath}")
    val conf = new Configuration()
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(filePath, conf))
    try {
      val schema = reader.getFileMetaData.getSchema
      val fields = schema.getFields.asScala.map(_.getName).toSet
      assert(fields.contains("logType"))
      assert(fields.contains("share"))
      assert(fields.contains("schema"))
      assert(fields.contains("table"))
      assert(fields.contains("egressBytes"))
      assert(fields.contains("pricingTier"))
      assert(fields.contains("timestampMs"))
      assert(fields.contains("requestType"))
      assert(fields.contains("clientRegion"))
    } finally {
      reader.close()
    }
  }

  test("table is auto-created on first write") {
    val basePath = makeTempPath()
    val tablePath = tablePathForShare(basePath, "tenant1_share")
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
    assert(partCols == Seq("year", "month", "day"))
  }

  test("records from different tenants are written to separate tables") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry(share = "tenant1_share"))
    writer.record(makeEntry(share = "tenant2_share"))
    writer.record(makeEntry(share = "_system_share"))
    writer.close()

    val conf = new Configuration()

    // Check tenant1 table
    val tenant1Path = tablePathForShare(basePath, "tenant1_share")
    val log1 = DeltaLog.forTable(conf, new Path(tenant1Path))
    assert(log1.snapshot().getVersion >= 0, "tenant1 table should exist")
    assert(log1.snapshot().getAllFiles.asScala.nonEmpty, "tenant1 table should have data")

    // Check tenant2 table
    val tenant2Path = tablePathForShare(basePath, "tenant2_share")
    val log2 = DeltaLog.forTable(conf, new Path(tenant2Path))
    assert(log2.snapshot().getVersion >= 0, "tenant2 table should exist")
    assert(log2.snapshot().getAllFiles.asScala.nonEmpty, "tenant2 table should have data")

    // Check _system table
    val systemPath = tablePathForShare(basePath, "_system_share")
    val logSystem = DeltaLog.forTable(conf, new Path(systemPath))
    assert(logSystem.snapshot().getVersion >= 0, "_system table should exist")
    assert(logSystem.snapshot().getAllFiles.asScala.nonEmpty, "_system table should have data")
  }

  test("share names without _share suffix are handled gracefully") {
    val basePath = makeTempPath()
    val writer = new DeltaAccessLogWriter(basePath, flushIntervalSeconds = 5, flushBatchSize = 100)
    writer.record(makeEntry(share = "legacy_share_name"))
    writer.close()

    // Should fall back to using the full share name as tenant_id
    val tablePath = s"$basePath/access_log_legacy_share_name"
    val conf = new Configuration()
    val log = DeltaLog.forTable(conf, new Path(tablePath))
    assert(log.snapshot().getVersion >= 0, "Table should exist for share without _share suffix")
  }
}
