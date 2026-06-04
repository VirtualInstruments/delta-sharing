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

import java.util.Base64

import org.scalatest.FunSuite

class GcpPricingTierSuite extends FunSuite {
  import GcpPricingTier._
  import GcpPricingTier.Continent._
  import GcpPricingTier.EgressType._

  // ===== Continent Mapping Tests =====

  test("continentFromGcpRegion maps US regions to NA") {
    assert(continentFromGcpRegion("us-central1") == NA)
    assert(continentFromGcpRegion("us-east1") == NA)
    assert(continentFromGcpRegion("us-west1-a") == NA)
  }

  test("continentFromGcpRegion maps Europe regions to EU") {
    assert(continentFromGcpRegion("europe-west1") == EU)
    assert(continentFromGcpRegion("europe-north1-b") == EU)
  }

  test("continentFromGcpRegion maps Asia regions to APAC") {
    assert(continentFromGcpRegion("asia-east1") == APAC)
    assert(continentFromGcpRegion("asia-southeast1") == APAC)
  }

  test("continentFromGcpRegion maps Australia regions to OCEANIA") {
    assert(continentFromGcpRegion("australia-southeast1") == OCEANIA)
  }

  test("continentFromGcpRegion maps South America regions to LATAM") {
    assert(continentFromGcpRegion("southamerica-east1") == LATAM)
  }

  test("continentFromGcpRegion handles null and empty") {
    assert(continentFromGcpRegion(null) == UNKNOWN_CONTINENT)
    assert(continentFromGcpRegion("") == UNKNOWN_CONTINENT)
    assert(continentFromGcpRegion("unknown-region") == UNKNOWN_CONTINENT)
  }

  test("continentFromCountryCode maps NA countries correctly") {
    assert(continentFromCountryCode("US") == NA)
    assert(continentFromCountryCode("CA") == NA)
    assert(continentFromCountryCode("MX") == NA)
  }

  test("continentFromCountryCode maps EU countries correctly") {
    assert(continentFromCountryCode("DE") == EU)
    assert(continentFromCountryCode("FR") == EU)
    assert(continentFromCountryCode("GB") == EU)
    assert(continentFromCountryCode("MT") == EU)
  }

  test("continentFromCountryCode maps APAC countries correctly") {
    assert(continentFromCountryCode("JP") == APAC)
    assert(continentFromCountryCode("SG") == APAC)
    assert(continentFromCountryCode("IN") == APAC)
  }

  test("continentFromCountryCode maps Oceania countries correctly") {
    assert(continentFromCountryCode("AU") == OCEANIA)
    assert(continentFromCountryCode("NZ") == OCEANIA)
  }

  test("continentFromCountryCode maps LATAM countries correctly") {
    assert(continentFromCountryCode("BR") == LATAM)
    assert(continentFromCountryCode("AR") == LATAM)
  }

  test("continentFromCountryCode handles ZZ (unknown) code") {
    assert(continentFromCountryCode("ZZ") == UNKNOWN_CONTINENT)
  }

  test("continentFromCountryCode is case-insensitive") {
    assert(continentFromCountryCode("us") == NA)
    assert(continentFromCountryCode("Us") == NA)
  }

  // ===== Pricing Tier Calculation Tests =====

  test("calculatePricingTier returns same_zone for SAME_ZONE egress type") {
    val tier = calculatePricingTier(
      sourceRegion = Some("us-central1"),
      destinationRegion = None,
      destinationCountry = None,
      egressType = SAME_ZONE)
    assert(tier == TIER_SAME_ZONE)
  }

  test("calculatePricingTier returns same_region for SAME_REGION egress type") {
    val tier = calculatePricingTier(
      sourceRegion = Some("us-central1"),
      destinationRegion = None,
      destinationCountry = None,
      egressType = SAME_REGION)
    assert(tier == TIER_SAME_REGION)
  }

  test("calculatePricingTier returns interregion_na_na for NA to NA inter-region") {
    val tier = calculatePricingTier(
      sourceRegion = Some("us-central1"),
      destinationRegion = Some("us-east1"),
      destinationCountry = None,
      egressType = INTER_REGION)
    assert(tier == TIER_INTERREGION_NA_NA)
  }

  test("calculatePricingTier returns interregion_na_eu for NA to EU inter-region") {
    val tier = calculatePricingTier(
      sourceRegion = Some("us-central1"),
      destinationRegion = Some("europe-west1"),
      destinationCountry = None,
      egressType = INTER_REGION)
    assert(tier == TIER_INTERREGION_NA_EU)
  }

  test("calculatePricingTier returns interregion_eu_na for EU to NA inter-region") {
    val tier = calculatePricingTier(
      sourceRegion = Some("europe-west1"),
      destinationRegion = Some("us-east1"),
      destinationCountry = None,
      egressType = INTER_REGION)
    assert(tier == TIER_INTERREGION_EU_NA)
  }

  test("calculatePricingTier returns interregion_apac for Asia to Asia inter-region") {
    val tier = calculatePricingTier(
      sourceRegion = Some("asia-east1"),
      destinationRegion = Some("asia-southeast1"),
      destinationCountry = None,
      egressType = INTER_REGION)
    assert(tier == TIER_INTERREGION_APAC)
  }

  test("calculatePricingTier returns interregion_to_oceania for any to Oceania inter-region") {
    val tier = calculatePricingTier(
      sourceRegion = Some("us-central1"),
      destinationRegion = Some("australia-southeast1"),
      destinationCountry = None,
      egressType = INTER_REGION)
    assert(tier == TIER_INTERREGION_TO_OCEANIA)
  }

  test("calculatePricingTier returns internet_to_na_eu for internet egress to NA/EU") {
    val tierNA = calculatePricingTier(
      sourceRegion = Some("europe-west1"),
      destinationRegion = None,
      destinationCountry = Some("US"),
      egressType = INTERNET)
    assert(tierNA == TIER_INTERNET_NA_EU)

    val tierEU = calculatePricingTier(
      sourceRegion = Some("us-central1"),
      destinationRegion = None,
      destinationCountry = Some("DE"),
      egressType = INTERNET)
    assert(tierEU == TIER_INTERNET_NA_EU)
  }

  test("calculatePricingTier returns internet_apac for internet egress to Asia") {
    val tier = calculatePricingTier(
      sourceRegion = Some("us-central1"),
      destinationRegion = None,
      destinationCountry = Some("JP"),
      egressType = INTERNET)
    assert(tier == TIER_INTERNET_APAC)
  }

  test("calculatePricingTier returns internet_latam for internet egress to LATAM") {
    val tier = calculatePricingTier(
      sourceRegion = Some("us-central1"),
      destinationRegion = None,
      destinationCountry = Some("BR"),
      egressType = INTERNET)
    assert(tier == TIER_INTERNET_LATAM)
  }

  test("calculatePricingTier returns internet_oceania for internet egress to Oceania") {
    val tier = calculatePricingTier(
      sourceRegion = Some("us-central1"),
      destinationRegion = None,
      destinationCountry = Some("AU"),
      egressType = INTERNET)
    assert(tier == TIER_INTERNET_OCEANIA)
  }

  // ===== Egress Type Detection Tests =====

  test("isLikelySameClusterTraffic returns true for missing client IP") {
    assert(isLikelySameClusterTraffic(None))
  }

  test("isLikelySameClusterTraffic returns true for private IPs") {
    assert(isLikelySameClusterTraffic(Some("10.0.0.1")))
    assert(isLikelySameClusterTraffic(Some("172.16.0.1")))
    assert(isLikelySameClusterTraffic(Some("192.168.1.1")))
    assert(isLikelySameClusterTraffic(Some("127.0.0.1")))
  }

  test("isLikelySameClusterTraffic returns false for public IPs") {
    assert(!isLikelySameClusterTraffic(Some("8.8.8.8")))
    assert(!isLikelySameClusterTraffic(Some("1.2.3.4")))
  }

  test("determineEgressType returns SAME_REGION for same-cluster traffic") {
    val (egressType, destRegion) = determineEgressType(
      clientIp = None,
      envoyPeerMetadata = None,
      clientRegion = None,
      detectGcpTraffic = true)
    assert(egressType == SAME_REGION)
    assert(destRegion.isEmpty)
  }

  test("determineEgressType returns INTER_REGION when ZZ region detected") {
    val (egressType, destRegion) = determineEgressType(
      clientIp = Some("34.100.0.1"),
      envoyPeerMetadata = None,
      clientRegion = Some("ZZ"),
      detectGcpTraffic = true)
    assert(egressType == INTER_REGION)
    assert(destRegion.isEmpty)
  }

  test("determineEgressType returns INTERNET when valid country code present") {
    val (egressType, destRegion) = determineEgressType(
      clientIp = Some("1.2.3.4"),
      envoyPeerMetadata = None,
      clientRegion = Some("US"),
      detectGcpTraffic = true)
    assert(egressType == INTERNET)
    assert(destRegion.isEmpty)
  }

  test("determineEgressType returns SAME_REGION when client GCP region matches source region") {
    // Create Envoy metadata with gcp_location
    val payload = """{"gcp_location":"us-central1-f"}"""
    val encoded = Base64.getEncoder.encodeToString(payload.getBytes("UTF-8"))

    val (egressType, destRegion) = determineEgressType(
      clientIp = Some("34.100.0.1"),
      envoyPeerMetadata = Some(encoded),
      clientRegion = Some("US"),
      detectGcpTraffic = true,
      sourceRegion = Some("us-central1"))
    assert(egressType == SAME_REGION)
    assert(destRegion.contains("us-central1"))
  }

  test("determineEgressType returns INTER_REGION when client GCP region differs from source region") {
    // Create Envoy metadata with gcp_location in different region
    val payload = """{"gcp_location":"europe-west1-b"}"""
    val encoded = Base64.getEncoder.encodeToString(payload.getBytes("UTF-8"))

    val (egressType, destRegion) = determineEgressType(
      clientIp = Some("34.100.0.1"),
      envoyPeerMetadata = Some(encoded),
      clientRegion = Some("DE"),
      detectGcpTraffic = true,
      sourceRegion = Some("us-central1"))
    assert(egressType == INTER_REGION)
    assert(destRegion.contains("europe-west1"))
  }

  test("determineEgressType returns SAME_REGION with zone suffix variations") {
    // Test that "us-central1" matches "us-central1-f"
    val payload = """{"gcp_location":"us-central1-a"}"""
    val encoded = Base64.getEncoder.encodeToString(payload.getBytes("UTF-8"))

    val (egressType, destRegion) = determineEgressType(
      clientIp = Some("34.100.0.1"),
      envoyPeerMetadata = Some(encoded),
      clientRegion = Some("US"),
      detectGcpTraffic = true,
      sourceRegion = Some("us-central1-f"))
    assert(egressType == SAME_REGION)
    assert(destRegion.contains("us-central1"))
  }

  // ===== Envoy Metadata Parsing Tests =====

  test("extractGcpRegionFromEnvoyMetadata returns None for null/empty") {
    assert(extractGcpRegionFromEnvoyMetadata(null).isEmpty)
    assert(extractGcpRegionFromEnvoyMetadata("").isEmpty)
  }

  test("extractGcpRegionFromEnvoyMetadata extracts region from base64 metadata") {
    // Create a simple test payload with gcp_location
    val payload = """{"gcp_location":"us-central1-f"}"""
    val encoded = Base64.getEncoder.encodeToString(payload.getBytes("UTF-8"))
    val result = extractGcpRegionFromEnvoyMetadata(encoded)
    assert(result.contains("us-central1"))
  }

  test("extractGcpRegionFromEnvoyMetadata strips zone suffix") {
    val payload = """gcp_location:europe-west1-b"""
    val encoded = Base64.getEncoder.encodeToString(payload.getBytes("UTF-8"))
    val result = extractGcpRegionFromEnvoyMetadata(encoded)
    assert(result.contains("europe-west1"))
  }

  test("extractGcpRegionFromEnvoyMetadata handles protobuf binary format") {
    // Simulate protobuf binary where field name and value are separated by control chars
    // gcp_location + [0x12 0x0f 0x1a 0x0d] + us-central1-f
    val binaryPayload = "gcp_location\u0012\u000f\u001a\rus-central1-f"
    val encoded = Base64.getEncoder.encodeToString(binaryPayload.getBytes("UTF-8"))
    val result = extractGcpRegionFromEnvoyMetadata(encoded)
    assert(result.contains("us-central1"))
  }

  test("extractGcpRegionFromEnvoyMetadata parses real Istio/ASM metadata") {
    // Real X-Envoy-Peer-Metadata from Istio ingress gateway (GKE with ASM)
    // Contains PLATFORM_METADATA with gcp_location:us-central1-f
    val realMetadata = "ChQKDkFQUF9DT05UQUlORVJTEgIaAAo9CgpDTFVTVEVSX0lEEi8aLWNuLXppbmct" +
      "ZGV2LTE5NzUyMi11cy1jZW50cmFsMS1mLXppbmctY2x1c3RlcgoeCgxJTlNUQU5DRV9JUFMSDhoM" +
      "MTAuMjUyLjguMTI3CiAKDUlTVElPX1ZFUlNJT04SDxoNMS4yMC44LWFzbS43MwqzAgoGTEFCRUxT" +
      "EqgCKqUCCh0KA2FwcBIWGhRpc3Rpby1pbmdyZXNzZ2F0ZXdheQoZCgVpc3RpbxIQGg5pbmdyZXNz" +
      "Z2F0ZXdheQodCgxpc3Rpby5pby9yZXYSDRoLYXNtLW1hbmFnZWQKOQofc2VydmljZS5pc3Rpby5p" +
      "by9jYW5vbmljYWwtbmFtZRIWGhRpc3Rpby1pbmdyZXNzZ2F0ZXdheQovCiNzZXJ2aWNlLmlzdGlv" +
      "LmlvL2Nhbm9uaWNhbC1yZXZpc2lvbhIIGgZsYXRlc3QKLgoddG9wb2xvZ3kua3ViZXJuZXRlcy5p" +
      "by9yZWdpb24SDRoLdXMtY2VudHJhbDEKLgobdG9wb2xvZ3kua3ViZXJuZXRlcy5pby96b25lEg8a" +
      "DXVzLWNlbnRyYWwxLWMKHgoHTUVTSF9JRBITGhFwcm9qLTMwMzkzMzg2ODgxMAovCgROQU1FEica" +
      "JWlzdGlvLWluZ3Jlc3NnYXRld2F5LTY2NjRjZGRkN2YtamM5bTIKGwoJTkFNRVNQQUNFEg4aDGlz" +
      "dGlvLXN5c3RlbQpdCgVPV05FUhJUGlJrdWJlcm5ldGVzOi8vYXBpcy9hcHBzL3YxL25hbWVzcGFj" +
      "ZXMvaXN0aW8tc3lzdGVtL2RlcGxveW1lbnRzL2lzdGlvLWluZ3Jlc3NnYXRld2F5CrACChFQTEFU" +
      "Rk9STV9NRVRBREFUQRKaAiqXAgomChRnY3BfZ2tlX2NsdXN0ZXJfbmFtZRIOGgx6aW5nLWNsdXN0" +
      "ZXIKgwEKE2djcF9na2VfY2x1c3Rlcl91cmwSbBpqaHR0cHM6Ly9jb250YWluZXIuZ29vZ2xlYXBp" +
      "cy5jb20vdjEvcHJvamVjdHMvemluZy1kZXYtMTk3NTIyL2xvY2F0aW9ucy91cy1jZW50cmFsMS1m" +
      "L2NsdXN0ZXJzL3ppbmctY2x1c3RlcgofCgxnY3BfbG9jYXRpb24SDxoNdXMtY2VudHJhbDEtZgog" +
      "CgtnY3BfcHJvamVjdBIRGg96aW5nLWRldi0xOTc1MjIKJAoSZ2NwX3Byb2plY3RfbnVtYmVyEg4a" +
      "DDMwMzkzMzg2ODgxMAonCg1XT1JLTE9BRF9OQU1FEhYaFGlzdGlvLWluZ3Jlc3NnYXRld2F5"
    val result = extractGcpRegionFromEnvoyMetadata(realMetadata)
    assert(result.contains("us-central1"), s"Expected us-central1 but got $result")
  }
}
