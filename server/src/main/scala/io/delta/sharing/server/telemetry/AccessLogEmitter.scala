/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import org.slf4j.LoggerFactory

import io.delta.sharing.server.common.JsonUtils
import io.delta.sharing.server.config.ServerConfig

/**
 * Represents a single access log entry for share data egress tracking.
 *
 * @param share The name of the share being accessed
 * @param schema The schema containing the table
 * @param table The table being accessed
 * @param egressBytes The number of bytes transferred
 * @param requestType The type of request: "query" or "cdf_stream"
 * @param timestampMs The timestamp of the access in milliseconds since epoch
 */
case class AccessLogEntry(
    share: String,
    schema: String,
    table: String,
    egressBytes: Long,
    requestType: String,
    timestampMs: Long)

/**
 * Trait for emitting access logs for share data egress tracking.
 * Implementations can write to different destinations (logging, external services, etc.)
 */
trait AccessLogEmitter {
  def record(entry: AccessLogEntry): Unit
}

object AccessLogEmitter {
  val QueryRequestType = "query"
  val CdfStreamRequestType = "cdf_stream"

  /**
   * Creates an AccessLogEmitter based on server configuration.
   * Returns a JsonAccessLogEmitter if access logging is enabled, otherwise a no-op emitter.
   */
  def create(serverConfig: ServerConfig): AccessLogEmitter = {
    val cfg = Option(serverConfig.getAccessLogging)
    cfg match {
      case Some(c) if c.enabled => new JsonAccessLogEmitter()
      case _ => NoopAccessLogEmitter
    }
  }
}

/**
 * No-op implementation for when access logging is disabled.
 */
object NoopAccessLogEmitter extends AccessLogEmitter {
  override def record(entry: AccessLogEntry): Unit = {}
}

/**
 * Emits access log entries as JSON-structured log lines.
 * Uses a dedicated logger that can be filtered/routed separately in Cloud Logging.
 */
class JsonAccessLogEmitter extends AccessLogEmitter {
  // Dedicated logger for access logs - can be filtered in Cloud Logging by logger name
  private val logger = LoggerFactory.getLogger("delta.sharing.access")

  override def record(entry: AccessLogEntry): Unit = {
    if (entry.egressBytes <= 0) {
      return
    }

    val logPayload = Map(
      "logType" -> "ACCESS_LOG",
      "share" -> entry.share,
      "schema" -> entry.schema,
      "table" -> entry.table,
      "egressBytes" -> entry.egressBytes,
      "requestType" -> entry.requestType,
      "timestampMs" -> entry.timestampMs
    )

    logger.info(JsonUtils.toJson(logPayload))
  }
}
