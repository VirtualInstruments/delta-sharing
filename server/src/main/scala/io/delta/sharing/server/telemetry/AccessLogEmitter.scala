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
 * == Core Fields (always present) ==
 * @param share The name of the share being accessed
 * @param schema The schema containing the table
 * @param table The table being accessed
 * @param egressBytes The number of bytes transferred (used for cost calculation)
 * @param timestampMs The timestamp of the access in milliseconds since epoch
 *
 * == Pricing Fields ==
 * @param pricingTier GCP egress pricing tier. See GcpPricingTier for values:
 *
 *   '''Internet Egress (Premium Tier):'''
 *   - `internet_to_na_eu` - To North America or Europe (~$0.12/GiB)
 *   - `internet_to_apac` - To Asia Pacific (~$0.12/GiB)
 *   - `internet_to_latam` - To Latin America (~$0.19/GiB)
 *   - `internet_to_oceania` - To Australia/Oceania (~$0.15/GiB)
 *
 *   '''Inter-region GCP Traffic:'''
 *   - `interregion_na_to_na` - NA to NA (~$0.02/GiB)
 *   - `interregion_na_to_eu` - NA to EU (~$0.05/GiB)
 *   - `interregion_eu_to_na` - EU to NA (~$0.05/GiB)
 *   - `interregion_eu_to_eu` - EU to EU (~$0.02/GiB)
 *   - `interregion_to_apac` - Any to APAC (~$0.08/GiB)
 *   - `interregion_to_oceania` - Any to Oceania (~$0.10/GiB)
 *   - `interregion_to_latam` - Any to LATAM (~$0.14/GiB)
 *
 *   '''Free/Low-cost:'''
 *   - `same_zone` - Within same GCP zone (free)
 *   - `same_region` - Within same region or K8s cluster (~free)
 *
 *   - `unknown` - Could not determine pricing tier
 *
 *   Pricing reference: https://cloud.google.com/vpc/network-pricing
 *
 * == Optional Context ==
 * @param clientRegion ISO 3166-1 alpha-2 country code of the client (e.g., "US", "MT")
 * @param requestType Type of request: "query" (snapshot read) or "cdf_stream" (CDF streaming)
 */
case class AccessLogEntry(
    share: String,
    schema: String,
    table: String,
    egressBytes: Long,
    timestampMs: Long,
    pricingTier: String = "unknown",
    clientRegion: Option[String] = None,
    requestType: String = "query")

/**
 * Captures all context information used to calculate the pricing tier.
 * Emitted alongside ACCESS_LOG for debugging and accuracy verification.
 *
 * @param share The share being accessed (for correlation)
 * @param table The table being accessed (for correlation)
 * @param timestampMs Timestamp for correlation with ACCESS_LOG
 *
 * == Raw Header Values ==
 * @param clientIp First non-private, non-GCP IP from X-Forwarded-For chain
 * @param clientIpSource Header that provided the client IP (e.g., "x-forwarded-for")
 * @param rawRegionHeader Raw value from region header (before normalization)
 * @param regionHeaderSource Header that provided the region (e.g., "x-client-region")
 * @param hasEnvoyMetadata Whether X-Envoy-Peer-Metadata header was present
 * @param isGcpIp Whether the client IP is in a known GCP public IP range (34.x, 35.x)
 *
 * == Derived Values ==
 * @param egressType Detected egress type: internet, inter_region, same_region, same_zone
 * @param sourceRegion GCP region where server runs (from config)
 * @param sourceContinent Continent of source region
 * @param destinationRegion GCP region of destination (if detected from Envoy metadata)
 * @param destinationContinent Continent of destination
 * @param pricingTier Final calculated pricing tier
 */
case class PricingContextLogEntry(
    share: String,
    table: String,
    timestampMs: Long,
    // Raw header values
    clientIp: Option[String] = None,
    clientIpSource: Option[String] = None,
    rawRegionHeader: Option[String] = None,
    regionHeaderSource: Option[String] = None,
    hasEnvoyMetadata: Boolean = false,
    isGcpIp: Boolean = false,
    // Derived values
    egressType: String = "unknown",
    sourceRegion: Option[String] = None,
    sourceContinent: Option[String] = None,
    destinationRegion: Option[String] = None,
    destinationContinent: Option[String] = None,
    pricingTier: String = "unknown")

/**
 * Trait for emitting access logs for share data egress tracking.
 * Implementations can write to different destinations (logging, external services, etc.)
 */
trait AccessLogEmitter {
  def record(entry: AccessLogEntry): Unit
  def recordContext(entry: PricingContextLogEntry): Unit
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
  override def recordContext(entry: PricingContextLogEntry): Unit = {}
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

    // Core payload with essential fields for cost tracking
    val basePayload = Map(
      "logType" -> "ACCESS_LOG",
      "share" -> entry.share,
      "schema" -> entry.schema,
      "table" -> entry.table,
      "egressBytes" -> entry.egressBytes,
      "pricingTier" -> entry.pricingTier,
      "timestampMs" -> entry.timestampMs,
      "requestType" -> entry.requestType
    )

    // Optional context fields
    val contextPayload = Seq(
      entry.clientRegion.map("clientRegion" -> _)
    ).flatten.toMap

    val logPayload = basePayload ++ contextPayload

    logger.info(JsonUtils.toJson(logPayload))
  }

  override def recordContext(entry: PricingContextLogEntry): Unit = {
    // Core payload for correlation
    val basePayload = Map(
      "logType" -> "PRICING_CONTEXT",
      "share" -> entry.share,
      "table" -> entry.table,
      "timestampMs" -> entry.timestampMs,
      "egressType" -> entry.egressType,
      "pricingTier" -> entry.pricingTier,
      "hasEnvoyMetadata" -> entry.hasEnvoyMetadata,
      "isGcpIp" -> entry.isGcpIp
    )

    // Optional context fields - only include if present
    val optionalPayload = Seq(
      entry.clientIp.map("clientIp" -> _),
      entry.clientIpSource.map("clientIpSource" -> _),
      entry.rawRegionHeader.map("rawRegionHeader" -> _),
      entry.regionHeaderSource.map("regionHeaderSource" -> _),
      entry.sourceRegion.map("sourceRegion" -> _),
      entry.sourceContinent.map("sourceContinent" -> _),
      entry.destinationRegion.map("destinationRegion" -> _),
      entry.destinationContinent.map("destinationContinent" -> _)
    ).flatten.toMap

    val logPayload = basePayload ++ optionalPayload

    logger.info(JsonUtils.toJson(logPayload))
  }
}
