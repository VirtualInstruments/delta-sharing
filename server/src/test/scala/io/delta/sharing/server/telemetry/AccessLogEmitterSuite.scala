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

import org.scalatest.FunSuite

import io.delta.sharing.server.config.{AccessLoggingConfig, ServerConfig}

class AccessLogEmitterSuite extends FunSuite {

  test("AccessLogEntry captures all required fields") {
    val entry = AccessLogEntry(
      share = "test-share",
      schema = "test-schema",
      table = "test-table",
      egressBytes = 1024L,
      timestampMs = System.currentTimeMillis(),
      pricingTier = "internet_to_na_eu",
      clientRegion = Some("US"),
      requestType = AccessLogEmitter.QueryRequestType
    )

    assert(entry.share == "test-share")
    assert(entry.schema == "test-schema")
    assert(entry.table == "test-table")
    assert(entry.egressBytes == 1024L)
    assert(entry.requestType == "query")
    assert(entry.clientRegion.contains("US"))
    assert(entry.pricingTier == "internet_to_na_eu")
  }

  test("NoopAccessLogEmitter ignores all records") {
    // Should not throw
    NoopAccessLogEmitter.record(AccessLogEntry(
      share = "s",
      schema = "sc",
      table = "t",
      egressBytes = 100L,
      timestampMs = 0L
    ))
  }

  test("JsonAccessLogEmitter skips zero-byte entries") {
    val emitter = new JsonAccessLogEmitter()
    // Should not throw, and should skip logging
    emitter.record(AccessLogEntry(
      share = "s",
      schema = "sc",
      table = "t",
      egressBytes = 0L,
      timestampMs = 0L
    ))
  }

  test("JsonAccessLogEmitter logs non-zero entries") {
    val emitter = new JsonAccessLogEmitter()
    // Should not throw
    emitter.record(AccessLogEntry(
      share = "customer-share",
      schema = "analytics",
      table = "events",
      egressBytes = 50000L,
      timestampMs = 1716864000000L,
      requestType = AccessLogEmitter.CdfStreamRequestType
    ))
  }

  test("AccessLogEmitter.create returns NoopAccessLogEmitter when disabled") {
    val config = new ServerConfig()
    // accessLogging is null by default
    val emitter = AccessLogEmitter.create(config)
    assert(emitter == NoopAccessLogEmitter)
  }

  test("AccessLogEmitter.create returns NoopAccessLogEmitter when explicitly disabled") {
    val config = new ServerConfig()
    val accessConfig = new AccessLoggingConfig()
    accessConfig.setEnabled(false)
    config.setAccessLogging(accessConfig)

    val emitter = AccessLogEmitter.create(config)
    assert(emitter == NoopAccessLogEmitter)
  }

  test("AccessLogEmitter.create returns JsonAccessLogEmitter when enabled") {
    val config = new ServerConfig()
    val accessConfig = new AccessLoggingConfig()
    accessConfig.setEnabled(true)
    config.setAccessLogging(accessConfig)

    val emitter = AccessLogEmitter.create(config)
    assert(emitter.isInstanceOf[JsonAccessLogEmitter])
  }

  test("AccessLogEmitter.create returns CompositeAccessLogEmitter when deltaTablePath is set") {
    val config = new ServerConfig()
    val accessConfig = new AccessLoggingConfig()
    accessConfig.setEnabled(true)
    accessConfig.setDeltaTablePath("/tmp/test-delta-table")
    config.setAccessLogging(accessConfig)

    val emitter = AccessLogEmitter.create(config)
    assert(emitter.isInstanceOf[CompositeAccessLogEmitter])
  }

  test("CompositeAccessLogEmitter fans out record calls to all delegates") {
    var count = 0
    val counting = new AccessLogEmitter {
      override def record(entry: AccessLogEntry): Unit = count += 1
      override def recordContext(entry: PricingContextLogEntry): Unit = {}
      override def recordHeaders(entry: RequestHeadersLogEntry): Unit = {}
    }
    val composite = new CompositeAccessLogEmitter(Seq(counting, counting))
    composite.record(AccessLogEntry("s", "sc", "t", 100L, 0L))
    assert(count == 2)
  }

  test("CompositeAccessLogEmitter close() calls close on all delegates") {
    var closedCount = 0
    val closeable = new AccessLogEmitter {
      override def record(entry: AccessLogEntry): Unit = {}
      override def recordContext(entry: PricingContextLogEntry): Unit = {}
      override def recordHeaders(entry: RequestHeadersLogEntry): Unit = {}
      override def close(): Unit = closedCount += 1
    }
    val composite = new CompositeAccessLogEmitter(Seq(closeable, closeable))
    composite.close()
    assert(closedCount == 2)
  }

  test("request type constants are defined") {
    assert(AccessLogEmitter.QueryRequestType == "query")
    assert(AccessLogEmitter.CdfStreamRequestType == "cdf_stream")
  }
}
