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

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class GcpIpRangeLookupSuite extends FunSuite with BeforeAndAfterEach {

  // Sample GCP IP ranges JSON for testing
  private val testRangesJson =
    """
    {
      "syncToken": "1234567890",
      "creationTime": "2026-06-01T00:00:00.000000",
      "prefixes": [
        {
          "ipv4Prefix": "34.44.0.0/15",
          "service": "Google Cloud",
          "scope": "us-central1"
        },
        {
          "ipv4Prefix": "34.72.0.0/16",
          "service": "Google Cloud",
          "scope": "us-central1"
        },
        {
          "ipv4Prefix": "34.73.0.0/16",
          "service": "Google Cloud",
          "scope": "us-east1"
        },
        {
          "ipv4Prefix": "35.187.0.0/17",
          "service": "Google Cloud",
          "scope": "europe-west1"
        },
        {
          "ipv4Prefix": "34.87.0.0/17",
          "service": "Google Cloud",
          "scope": "asia-southeast1"
        },
        {
          "ipv4Prefix": "34.151.64.0/18",
          "service": "Google Cloud",
          "scope": "australia-southeast1"
        },
        {
          "ipv4Prefix": "34.95.128.0/17",
          "service": "Google Cloud",
          "scope": "southamerica-east1"
        },
        {
          "ipv4Prefix": "34.8.0.0/16",
          "service": "Google Cloud",
          "scope": "global"
        },
        {
          "ipv6Prefix": "2600:1900:4000::/44",
          "service": "Google Cloud",
          "scope": "us-central1"
        }
      ]
    }
    """

  override def beforeEach(): Unit = {
    GcpIpRangeLookup.reset()
  }

  test("lookupRegion returns None for empty/null IP") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)
    assert(GcpIpRangeLookup.lookupRegion(null).isEmpty)
    assert(GcpIpRangeLookup.lookupRegion("").isEmpty)
  }

  test("lookupRegion returns None for non-GCP IPs") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)
    assert(GcpIpRangeLookup.lookupRegion("8.8.8.8").isEmpty)
    assert(GcpIpRangeLookup.lookupRegion("1.2.3.4").isEmpty)
    assert(GcpIpRangeLookup.lookupRegion("217.9.6.27").isEmpty) // Malta IP
  }

  test("lookupRegion returns correct region for us-central1 IP") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    // 34.44.0.0/15 covers 34.44.0.0 - 34.45.255.255
    assert(GcpIpRangeLookup.lookupRegion("34.44.0.1") == Some("us-central1"))
    assert(GcpIpRangeLookup.lookupRegion("34.45.22.184") == Some("us-central1"))
    assert(GcpIpRangeLookup.lookupRegion("34.45.255.255") == Some("us-central1"))

    // 34.72.0.0/16
    assert(GcpIpRangeLookup.lookupRegion("34.72.100.50") == Some("us-central1"))
  }

  test("lookupRegion returns correct region for us-east1 IP") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    // 34.73.0.0/16
    assert(GcpIpRangeLookup.lookupRegion("34.73.0.1") == Some("us-east1"))
    assert(GcpIpRangeLookup.lookupRegion("34.73.128.100") == Some("us-east1"))
  }

  test("lookupRegion returns correct region for europe-west1 IP") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    // 35.187.0.0/17 covers 35.187.0.0 - 35.187.127.255
    assert(GcpIpRangeLookup.lookupRegion("35.187.0.1") == Some("europe-west1"))
    assert(GcpIpRangeLookup.lookupRegion("35.187.64.100") == Some("europe-west1"))
    assert(GcpIpRangeLookup.lookupRegion("35.187.127.255") == Some("europe-west1"))

    // 35.187.128.0 is outside the /17
    assert(GcpIpRangeLookup.lookupRegion("35.187.128.0").isEmpty)
  }

  test("lookupRegion returns correct region for asia-southeast1 IP") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    // 34.87.0.0/17
    assert(GcpIpRangeLookup.lookupRegion("34.87.0.1") == Some("asia-southeast1"))
    assert(GcpIpRangeLookup.lookupRegion("34.87.100.50") == Some("asia-southeast1"))
  }

  test("lookupRegion returns correct region for australia-southeast1 IP") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    // 34.151.64.0/18 covers 34.151.64.0 - 34.151.127.255
    assert(GcpIpRangeLookup.lookupRegion("34.151.64.1") == Some("australia-southeast1"))
    assert(GcpIpRangeLookup.lookupRegion("34.151.100.50") == Some("australia-southeast1"))
  }

  test("lookupRegion returns correct region for southamerica-east1 IP") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    // 34.95.128.0/17
    assert(GcpIpRangeLookup.lookupRegion("34.95.128.1") == Some("southamerica-east1"))
    assert(GcpIpRangeLookup.lookupRegion("34.95.200.50") == Some("southamerica-east1"))
  }

  test("lookupRegion skips global scope ranges") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    // 34.8.0.0/16 is in the JSON but with scope "global" - should be skipped
    assert(GcpIpRangeLookup.lookupRegion("34.8.1.1").isEmpty)
  }

  test("isGcpIp returns true for GCP IPs") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    assert(GcpIpRangeLookup.isGcpIp("34.45.22.184"))
    assert(GcpIpRangeLookup.isGcpIp("34.73.100.50"))
    assert(GcpIpRangeLookup.isGcpIp("35.187.64.100"))
  }

  test("isGcpIp returns false for non-GCP IPs") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    assert(!GcpIpRangeLookup.isGcpIp("8.8.8.8"))
    assert(!GcpIpRangeLookup.isGcpIp("1.2.3.4"))
    assert(!GcpIpRangeLookup.isGcpIp("192.168.1.1"))
  }

  test("handles invalid IP addresses gracefully") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    assert(GcpIpRangeLookup.lookupRegion("not-an-ip").isEmpty)
    assert(GcpIpRangeLookup.lookupRegion("256.256.256.256").isEmpty)
    assert(GcpIpRangeLookup.lookupRegion("34.45").isEmpty)
  }

  test("handles IPv6 addresses") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)

    // 2600:1900:4000::/44 is us-central1
    assert(GcpIpRangeLookup.lookupRegion("2600:1900:4000::1") == Some("us-central1"))
    assert(GcpIpRangeLookup.lookupRegion("2600:1900:4001:1234::5678") == Some("us-central1"))

    // Outside the range
    assert(GcpIpRangeLookup.lookupRegion("2600:1901::1").isEmpty)
  }

  test("longest prefix match selects most specific range") {
    // Create test data with overlapping ranges
    val overlappingRangesJson =
      """
      {
        "syncToken": "123",
        "creationTime": "2026-06-01T00:00:00.000000",
        "prefixes": [
          {
            "ipv4Prefix": "34.0.0.0/8",
            "service": "Google Cloud",
            "scope": "us-west1"
          },
          {
            "ipv4Prefix": "34.45.0.0/16",
            "service": "Google Cloud",
            "scope": "us-central1"
          },
          {
            "ipv4Prefix": "34.45.22.0/24",
            "service": "Google Cloud",
            "scope": "us-east1"
          }
        ]
      }
      """
    GcpIpRangeLookup.loadFromJson(overlappingRangesJson)

    // Most specific match wins
    assert(GcpIpRangeLookup.lookupRegion("34.45.22.184") == Some("us-east1"))
    assert(GcpIpRangeLookup.lookupRegion("34.45.100.1") == Some("us-central1"))
    assert(GcpIpRangeLookup.lookupRegion("34.100.1.1") == Some("us-west1"))
  }

  test("returns None for IPs not in loaded ranges") {
    // After loading test ranges, IPs not in those ranges should return None
    GcpIpRangeLookup.loadFromJson(testRangesJson)
    // This IP is not in our test ranges
    assert(GcpIpRangeLookup.lookupRegion("34.200.200.200").isEmpty)
  }

  test("getLastRefreshTime updates after loadFromJson") {
    val timeBefore = GcpIpRangeLookup.getLastRefreshTime
    Thread.sleep(10)  // Small delay to ensure time difference
    GcpIpRangeLookup.loadFromJson(testRangesJson)
    assert(GcpIpRangeLookup.getLastRefreshTime > timeBefore)
  }

  test("getLastRefreshTime returns non-zero after load") {
    GcpIpRangeLookup.loadFromJson(testRangesJson)
    assert(GcpIpRangeLookup.getLastRefreshTime > 0)
  }
}
