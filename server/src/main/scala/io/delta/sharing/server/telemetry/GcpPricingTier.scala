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

import java.util.{Base64, Locale}

import scala.util.Try

/**
 * GCP pricing tier calculation for egress traffic.
 *
 * Implements classification of network egress into GCP pricing tiers based on:
 * - Source region (where the data originates, e.g., GCS bucket location)
 * - Destination region (where the client is located)
 * - Traffic type (same-zone, inter-region GCP, or internet)
 *
 * Pricing tiers are based on:
 * https://cloud.google.com/vpc/pricing-announce
 */
object GcpPricingTier {

  /**
   * Continent groupings for GCP regions and country codes.
   */
  object Continent extends Enumeration {
    type Continent = Value
    val NA, EU, APAC, LATAM, OCEANIA = Value
    val UNKNOWN_CONTINENT: Continent = Value("UNKNOWN")
  }

  import Continent._

  /**
   * Traffic types for egress pricing.
   */
  object EgressType extends Enumeration {
    type EgressType = Value
    val SAME_ZONE = Value("same_zone")
    val SAME_REGION = Value("same_region")
    val INTER_REGION = Value("inter_region")
    val INTERNET = Value("internet")
    val UNKNOWN_TYPE: EgressType = Value("unknown")
  }

  import EgressType._

  /**
   * Map GCP region prefix to continent.
   * GCP region format: {location}-{zone_letter}{zone_number} (e.g., us-central1-a)
   * We extract the location prefix.
   */
  private val GcpRegionToContinent: Map[String, Continent] = Map(
    // North America
    "us" -> NA,
    "northamerica" -> NA,
    // Europe
    "europe" -> EU,
    // Asia Pacific
    "asia" -> APAC,
    // South America
    "southamerica" -> LATAM,
    // Australia / Oceania
    "australia" -> OCEANIA
  )

  /**
   * Map ISO 3166-1 alpha-2 country codes to continents.
   * This covers the major countries; for others, we default to UNKNOWN.
   */
  private val CountryToContinent: Map[String, Continent] = Map(
    // North America
    "US" -> NA, "CA" -> NA, "MX" -> NA,
    // Europe
    "GB" -> EU, "IE" -> EU, "DE" -> EU, "FR" -> EU, "NL" -> EU, "BE" -> EU,
    "CH" -> EU, "AT" -> EU, "ES" -> EU, "PT" -> EU, "IT" -> EU, "SE" -> EU,
    "NO" -> EU, "DK" -> EU, "FI" -> EU, "PL" -> EU, "CZ" -> EU, "HU" -> EU,
    "RO" -> EU, "BG" -> EU, "GR" -> EU, "HR" -> EU, "SK" -> EU, "SI" -> EU,
    "EE" -> EU, "LV" -> EU, "LT" -> EU, "LU" -> EU, "MT" -> EU, "CY" -> EU,
    "IS" -> EU, "UA" -> EU, "BY" -> EU, "RU" -> EU,
    // Asia Pacific
    "JP" -> APAC, "KR" -> APAC, "CN" -> APAC, "HK" -> APAC, "TW" -> APAC,
    "SG" -> APAC, "MY" -> APAC, "ID" -> APAC, "TH" -> APAC, "VN" -> APAC,
    "PH" -> APAC, "IN" -> APAC, "PK" -> APAC, "BD" -> APAC,
    // Oceania
    "AU" -> OCEANIA, "NZ" -> OCEANIA,
    // Latin America
    "BR" -> LATAM, "AR" -> LATAM, "CL" -> LATAM, "CO" -> LATAM, "PE" -> LATAM,
    "VE" -> LATAM, "EC" -> LATAM, "BO" -> LATAM, "PY" -> LATAM, "UY" -> LATAM,
    "CR" -> LATAM, "PA" -> LATAM, "GT" -> LATAM, "HN" -> LATAM, "SV" -> LATAM,
    "NI" -> LATAM, "DO" -> LATAM, "CU" -> LATAM, "PR" -> LATAM
  )

  /**
   * Pricing tiers matching GCP network egress pricing structure.
   *
   * Inter-region format: interregion_{source}_{dest} or interregion_to_{dest}
   * Internet format: internet_to_{dest} (pricing based on destination only)
   * Special cases: same_zone, same_region (both essentially free)
   */
  val TIER_SAME_ZONE = "same_zone"
  val TIER_SAME_REGION = "same_region"
  val TIER_INTERREGION_NA_NA = "interregion_na_to_na"
  val TIER_INTERREGION_NA_EU = "interregion_na_to_eu"
  val TIER_INTERREGION_EU_NA = "interregion_eu_to_na"
  val TIER_INTERREGION_EU_EU = "interregion_eu_to_eu"
  val TIER_INTERREGION_APAC = "interregion_to_apac"
  val TIER_INTERREGION_TO_OCEANIA = "interregion_to_oceania"
  val TIER_INTERREGION_TO_LATAM = "interregion_to_latam"
  val TIER_INTERNET_NA_EU = "internet_to_na_eu"
  val TIER_INTERNET_APAC = "internet_to_apac"
  val TIER_INTERNET_LATAM = "internet_to_latam"
  val TIER_INTERNET_OCEANIA = "internet_to_oceania"
  val TIER_UNKNOWN = "unknown"

  /**
   * Extract continent from a GCP region string (e.g., "us-central1", "europe-west1-b").
   */
  def continentFromGcpRegion(region: String): Continent = {
    if (region == null || region.isEmpty) return UNKNOWN_CONTINENT

    val normalized = region.toLowerCase(Locale.ROOT)
    // GCP region format: {continent_prefix}-{location}{number}
    // e.g., us-central1, europe-west1, asia-east1, australia-southeast1
    val prefix = normalized.split("-").headOption.getOrElse("")

    GcpRegionToContinent.getOrElse(prefix, UNKNOWN_CONTINENT)
  }

  /**
   * Extract continent from an ISO 3166-1 alpha-2 country code.
   */
  def continentFromCountryCode(countryCode: String): Continent = {
    if (countryCode == null || countryCode.isEmpty) return UNKNOWN_CONTINENT

    val normalized = countryCode.toUpperCase(Locale.ROOT).trim
    // Handle special "ZZ" code (unknown location from GCP load balancer)
    if (normalized == "ZZ") return UNKNOWN_CONTINENT

    CountryToContinent.getOrElse(normalized, UNKNOWN_CONTINENT)
  }

  /**
   * Calculate the pricing tier based on source region, destination, and traffic type.
   *
   * @param sourceRegion     The GCP region where data originates (e.g., "us-central1")
   * @param destinationRegion Optional GCP region for inter-region GCP traffic
   * @param destinationCountry Optional ISO country code for internet traffic
   * @param egressType        The type of egress (same_zone, inter_region, internet)
   * @return The pricing tier string
   */
  def calculatePricingTier(
      sourceRegion: Option[String],
      destinationRegion: Option[String],
      destinationCountry: Option[String],
      egressType: EgressType): String = {

    egressType match {
      case SAME_ZONE => TIER_SAME_ZONE
      case SAME_REGION => TIER_SAME_REGION

      case INTER_REGION =>
        val srcContinent = sourceRegion
          .map(continentFromGcpRegion).getOrElse(UNKNOWN_CONTINENT)
        val dstContinent = destinationRegion
          .map(continentFromGcpRegion).getOrElse(UNKNOWN_CONTINENT)
        calculateInterRegionTier(srcContinent, dstContinent)

      case INTERNET =>
        // Internet egress pricing depends only on destination, not source
        val dstContinent = destinationCountry
          .map(continentFromCountryCode).getOrElse(UNKNOWN_CONTINENT)
        calculateInternetTier(dstContinent)

      case UNKNOWN_TYPE => TIER_UNKNOWN
    }
  }

  /**
   * Calculate inter-region GCP pricing tier based on source and destination continents.
   *
   * GCP inter-region pricing (approximate):
   * - NA to NA: $0.02/GiB
   * - NA to EU: $0.05/GiB
   * - EU to EU: $0.02/GiB
   * - To Asia: $0.08/GiB
   * - To Australia: $0.10/GiB
   * - To LATAM: $0.14/GiB
   */
  private def calculateInterRegionTier(src: Continent, dst: Continent): String = {
    (src, dst) match {
      case (NA, NA) => TIER_INTERREGION_NA_NA
      case (NA, EU) => TIER_INTERREGION_NA_EU
      case (EU, NA) => TIER_INTERREGION_EU_NA
      case (EU, EU) => TIER_INTERREGION_EU_EU
      case (APAC, APAC) => TIER_INTERREGION_APAC
      case (_, OCEANIA) => TIER_INTERREGION_TO_OCEANIA
      case (_, LATAM) => TIER_INTERREGION_TO_LATAM
      case (_, APAC) => TIER_INTERREGION_APAC
      case _ => TIER_UNKNOWN
    }
  }

  /**
   * Calculate internet egress pricing tier based on destination continent.
   * Note: GCP internet egress pricing depends only on destination, not source.
   *
   * GCP Premium Tier Internet Egress (approximate):
   * - To NA/EU: $0.12/GiB
   * - To Asia: $0.12/GiB
   * - To LATAM: $0.19/GiB
   * - To Australia: $0.15/GiB
   */
  def calculateInternetTier(dst: Continent): String = {
    dst match {
      case NA | EU => TIER_INTERNET_NA_EU
      case APAC => TIER_INTERNET_APAC
      case LATAM => TIER_INTERNET_LATAM
      case OCEANIA => TIER_INTERNET_OCEANIA
      case UNKNOWN_CONTINENT => TIER_UNKNOWN
    }
  }

  /**
   * Parse X-Envoy-Peer-Metadata header to extract GCP region.
   *
   * The header is base64-encoded and contains structured metadata including
   * a "gcp_location" field with the region (e.g., "us-central1-f").
   *
   * @param headerValue The base64-encoded X-Envoy-Peer-Metadata header value
   * @return Optional GCP region extracted from the metadata
   */
  def extractGcpRegionFromEnvoyMetadata(headerValue: String): Option[String] = {
    if (headerValue == null || headerValue.isEmpty) return None

    Try {
      val decoded = new String(Base64.getDecoder.decode(headerValue), "UTF-8")
      // The metadata is typically in a protobuf-like format or JSON
      // Look for patterns like "gcp_location":"us-central1-f" or gcp_location:us-central1-f
      val gcpLocationPattern = """(?:gcp_location|GCP_LOCATION)["\s:]+([a-z]+-[a-z0-9-]+)""".r
      gcpLocationPattern.findFirstMatchIn(decoded).map { m =>
        // Extract region without zone suffix (e.g., "us-central1" from "us-central1-f")
        val fullLocation = m.group(1)
        // Remove zone letter if present (e.g., -a, -b, -f)
        fullLocation.replaceAll("-[a-z]$", "")
      }
    }.toOption.flatten
  }

  /**
   * Check if an IP address appears to be from the same Kubernetes cluster.
   *
   * This is a heuristic check based on common pod CIDR ranges.
   * In practice, same-cluster traffic often has no X-Forwarded-For header
   * or has a private IP in the forwarding chain.
   *
   * @param clientIp The client IP address
   * @return true if the IP appears to be from the same cluster
   */
  def isLikelySameClusterTraffic(clientIp: Option[String]): Boolean = {
    clientIp match {
      case None => true // No forwarding header often means same-cluster
      case Some(ip) =>
        // Check for common private IP ranges used in Kubernetes
        ip.startsWith("10.") ||
        ip.startsWith("172.16.") || ip.startsWith("172.17.") || ip.startsWith("172.18.") ||
        ip.startsWith("172.19.") || ip.startsWith("172.20.") || ip.startsWith("172.21.") ||
        ip.startsWith("172.22.") || ip.startsWith("172.23.") || ip.startsWith("172.24.") ||
        ip.startsWith("172.25.") || ip.startsWith("172.26.") || ip.startsWith("172.27.") ||
        ip.startsWith("172.28.") || ip.startsWith("172.29.") || ip.startsWith("172.30.") ||
        ip.startsWith("172.31.") ||
        ip.startsWith("192.168.") ||
        ip == "127.0.0.1" || ip == "::1"
    }
  }

  /**
   * Determine the egress type based on available information.
   *
   * @param clientIp             The client IP address (from X-Forwarded-For)
   * @param envoyPeerMetadata    The X-Envoy-Peer-Metadata header value
   * @param clientRegion         The client region from X-Client-Region header
   * @param detectGcpTraffic     Whether GCP traffic detection is enabled
   * @return Tuple of (EgressType, Optional destination GCP region)
   */
  def determineEgressType(
      clientIp: Option[String],
      envoyPeerMetadata: Option[String],
      clientRegion: Option[String],
      detectGcpTraffic: Boolean): (EgressType, Option[String]) = {

    // Check for same-cluster traffic first
    if (isLikelySameClusterTraffic(clientIp)) {
      return (SAME_REGION, None)
    }

    // If GCP traffic detection is enabled, try to extract region from Envoy metadata
    if (detectGcpTraffic) {
      val gcpRegion = envoyPeerMetadata.flatMap(extractGcpRegionFromEnvoyMetadata)
      if (gcpRegion.isDefined) {
        return (INTER_REGION, gcpRegion)
      }
    }

    // Check if X-Client-Region indicates unknown/internal (ZZ)
    clientRegion match {
      // scalastyle:off caselocale
      case Some(region) if region.toUpperCase == "ZZ" =>
      // scalastyle:on caselocale
        // ZZ typically means GCP couldn't determine the location (internal traffic)
        (INTER_REGION, None)
      case Some(_) =>
        // Has a valid country code - this is internet traffic
        (INTERNET, None)
      case None =>
        // No region header - could be internal or misconfigured
        (UNKNOWN_TYPE, None)
    }
  }
}
