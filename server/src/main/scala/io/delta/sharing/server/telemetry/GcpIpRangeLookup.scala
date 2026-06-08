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

import java.net.{HttpURLConnection, InetAddress, URL}
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import io.delta.sharing.server.common.JsonUtils

/**
 * GCP IP Range lookup utility.
 *
 * Fetches GCP's published IP ranges from https://www.gstatic.com/ipranges/cloud.json
 * and provides efficient IP-to-region lookup using a CIDR trie.
 *
 * The IP ranges are refreshed periodically (default: every 24 hours) to pick up
 * any new allocations from GCP.
 */
object GcpIpRangeLookup {

  private val GCP_IP_RANGES_URL = "https://www.gstatic.com/ipranges/cloud.json"
  private val REFRESH_INTERVAL_HOURS = 24
  private val CONNECTION_TIMEOUT_MS = 10000
  private val READ_TIMEOUT_MS = 30000

  /**
   * Case class representing a single IP prefix entry from GCP's JSON.
   */
  case class IpPrefix(
      ipv4Prefix: Option[String] = None,
      ipv6Prefix: Option[String] = None,
      service: String = "",
      scope: String = ""
  )

  /**
   * Case class representing the full GCP IP ranges response.
   */
  case class GcpIpRanges(
      syncToken: String = "",
      creationTime: String = "",
      prefixes: Seq[IpPrefix] = Seq.empty
  )

  /**
   * A node in the CIDR trie for efficient IP-to-region lookup.
   */
  private class CidrTrieNode {
    var region: Option[String] = None
    var children: Array[CidrTrieNode] = null

    def getOrCreateChild(bit: Int): CidrTrieNode = {
      if (children == null) {
        children = new Array[CidrTrieNode](2)
      }
      if (children(bit) == null) {
        children(bit) = new CidrTrieNode()
      }
      children(bit)
    }

    def getChild(bit: Int): Option[CidrTrieNode] = {
      if (children == null) None
      else Option(children(bit))
    }
  }

  /**
   * CIDR Trie for efficient longest-prefix-match lookups.
   */
  private class CidrTrie {
    private val root = new CidrTrieNode()

    /**
     * Insert a CIDR block with its associated region.
     */
    def insert(cidr: String, region: String): Unit = {
      parseCidr(cidr).foreach { case (ipBytes, prefixLen) =>
        var node = root
        for (i <- 0 until prefixLen) {
          val byteIndex = i / 8
          val bitIndex = 7 - (i % 8)
          val bit = (ipBytes(byteIndex) >> bitIndex) & 1
          node = node.getOrCreateChild(bit)
        }
        node.region = Some(region)
      }
    }

    /**
     * Look up the region for an IP address using longest-prefix-match.
     */
    def lookup(ip: String): Option[String] = {
      parseIp(ip).flatMap { ipBytes =>
        var node = root
        var lastMatch: Option[String] = None
        var i = 0
        val maxBits = ipBytes.length * 8

        while (i < maxBits && node != null) {
          // Record the region if this node has one (longest prefix match)
          if (node.region.isDefined) {
            lastMatch = node.region
          }

          val byteIndex = i / 8
          val bitIndex = 7 - (i % 8)
          val bit = (ipBytes(byteIndex) >> bitIndex) & 1

          node.getChild(bit) match {
            case Some(child) =>
              node = child
              i += 1
            case None =>
              node = null
          }
        }

        // Check if final node has a region
        if (node != null && node.region.isDefined) {
          lastMatch = node.region
        }

        lastMatch
      }
    }

    /**
     * Parse a CIDR notation string into (ip bytes, prefix length).
     */
    private def parseCidr(cidr: String): Option[(Array[Byte], Int)] = {
      Try {
        val parts = cidr.split("/")
        val ipBytes = InetAddress.getByName(parts(0)).getAddress
        val prefixLen = parts(1).toInt
        (ipBytes, prefixLen)
      }.toOption
    }

    /**
     * Parse an IP address string into bytes.
     */
    private def parseIp(ip: String): Option[Array[Byte]] = {
      Try(InetAddress.getByName(ip).getAddress).toOption
    }
  }

  // The cached trie, atomically updated
  private val cachedTrie = new AtomicReference[CidrTrie](new CidrTrie())
  private val lastRefreshTime = new AtomicReference[Long](0L)
  private val isInitialized = new AtomicReference[Boolean](false)

  // Background refresh scheduler
  private lazy val scheduler = {
    val s = Executors.newSingleThreadScheduledExecutor((r: Runnable) => {
      val t = new Thread(r, "gcp-ip-range-refresh")
      t.setDaemon(true)
      t
    })
    s.scheduleAtFixedRate(
      () => refresh(),
      REFRESH_INTERVAL_HOURS,
      REFRESH_INTERVAL_HOURS,
      TimeUnit.HOURS
    )
    s
  }

  /**
   * Look up the GCP region for a given IP address.
   *
   * @param ip The IP address to look up (IPv4 or IPv6)
   * @return Optional GCP region (e.g., "us-central1", "europe-west1")
   */
  def lookupRegion(ip: String): Option[String] = {
    ensureInitialized()
    cachedTrie.get().lookup(ip)
  }

  /**
   * Check if an IP is in any GCP range.
   */
  def isGcpIp(ip: String): Boolean = {
    lookupRegion(ip).isDefined
  }

  /**
   * Ensure the IP ranges are loaded at least once.
   */
  def ensureInitialized(): Unit = {
    if (!isInitialized.get()) {
      synchronized {
        if (!isInitialized.get()) {
          refresh()
          // Touch the scheduler to start background refreshes
          scheduler
        }
      }
    }
  }

  /**
   * Refresh the IP ranges from GCP's endpoint.
   * This is called automatically on a schedule, but can also be called manually.
   */
  def refresh(): Unit = {
    try {
      fetchAndParse() match {
        case Success(ranges) =>
          val newTrie = buildTrie(ranges)
          cachedTrie.set(newTrie)
          lastRefreshTime.set(System.currentTimeMillis())
          isInitialized.set(true)
          // scalastyle:off println
          System.out.println(s"[GcpIpRangeLookup] Loaded ${ranges.prefixes.size} IP ranges " +
            s"(syncToken: ${ranges.syncToken})")
          // scalastyle:on println
        case Failure(e) =>
          // scalastyle:off println
          System.err.println(s"[GcpIpRangeLookup] Failed to refresh IP ranges: ${e.getMessage}")
          // scalastyle:on println
          // Keep using the old trie if we have one
          if (!isInitialized.get()) {
            // First-time failure - create empty trie
            cachedTrie.set(new CidrTrie())
            isInitialized.set(true)
          }
      }
    } catch {
      case NonFatal(e) =>
        // scalastyle:off println
        System.err.println(s"[GcpIpRangeLookup] Unexpected error during refresh: ${e.getMessage}")
        // scalastyle:on println
    }
  }

  /**
   * Fetch and parse the GCP IP ranges JSON.
   */
  private def fetchAndParse(): Try[GcpIpRanges] = {
    Try {
      val url = new URL(GCP_IP_RANGES_URL)
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setConnectTimeout(CONNECTION_TIMEOUT_MS)
      conn.setReadTimeout(READ_TIMEOUT_MS)
      conn.setRequestProperty("Accept", "application/json")

      try {
        val responseCode = conn.getResponseCode
        if (responseCode != 200) {
          throw new RuntimeException(s"HTTP $responseCode from GCP IP ranges endpoint")
        }

        val inputStream = conn.getInputStream
        val content = scala.io.Source.fromInputStream(inputStream).mkString
        inputStream.close()

        JsonUtils.fromJson[GcpIpRanges](content)
      } finally {
        conn.disconnect()
      }
    }
  }

  /**
   * Build a CIDR trie from the parsed IP ranges.
   */
  private def buildTrie(ranges: GcpIpRanges): CidrTrie = {
    val trie = new CidrTrie()

    for (prefix <- ranges.prefixes) {
      // Only include "Google Cloud" service entries with a valid scope
      // Skip "global" scope as it doesn't help with region detection
      if (prefix.service == "Google Cloud" && prefix.scope.nonEmpty &&
          prefix.scope != "global") {
        prefix.ipv4Prefix.foreach(cidr => trie.insert(cidr, prefix.scope))
        prefix.ipv6Prefix.foreach(cidr => trie.insert(cidr, prefix.scope))
      }
    }

    trie
  }

  /**
   * Get the time of the last successful refresh, in milliseconds since epoch.
   * Returns 0 if never refreshed.
   */
  def getLastRefreshTime: Long = lastRefreshTime.get()

  /**
   * For testing: load IP ranges from a pre-fetched JSON string.
   */
  def loadFromJson(json: String): Unit = {
    val ranges = JsonUtils.fromJson[GcpIpRanges](json)
    val newTrie = buildTrie(ranges)
    cachedTrie.set(newTrie)
    lastRefreshTime.set(System.currentTimeMillis())
    isInitialized.set(true)
  }

  /**
   * For testing: reset to uninitialized state.
   */
  def reset(): Unit = {
    cachedTrie.set(new CidrTrie())
    lastRefreshTime.set(0L)
    isInitialized.set(false)
  }
}
