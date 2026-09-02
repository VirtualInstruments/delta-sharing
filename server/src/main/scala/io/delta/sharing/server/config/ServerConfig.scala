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

package io.delta.sharing.server.config

import java.io.{File, IOException}
import java.util.Collections

import scala.beans.BeanProperty

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

/** A trait that requires to implement */
trait ConfigItem {
  /** Verify whether the config is valid */
  def checkConfig(): Unit
}

/**
 * The class for the server config yaml file. The yaml file will be loaded as this class.
 *
 * As `jackson-dataformat-yaml` only supports Java,  we need to use `@BeanProperty var` to generate
 * Java bean classes.
 */
case class ServerConfig(
    @BeanProperty var version: java.lang.Integer,
    @BeanProperty var shares: java.util.List[ShareConfig],
    @BeanProperty var authorization: Authorization,
    @BeanProperty var ssl: SSLConfig,
    @BeanProperty var host: String,
    @BeanProperty var port: Int,
    @BeanProperty var endpoint: String,
    // The timeout of S3 presigned url in seconds
    @BeanProperty var preSignedUrlTimeoutSeconds: Long,
    // How many tables to cache in the memory.
    @BeanProperty var deltaTableCacheSize: Int,
    // Whether we can accept working with a stale version of the table. This is useful when sharing
    // static tables that will never be changed.
    @BeanProperty var stalenessAcceptable: Boolean,
    // Whether to evaluate user provided `predicateHints`
    @BeanProperty var evaluatePredicateHints: Boolean,
    // Whether to evaluate user provided `jsonPredicateHints`
    @BeanProperty var evaluateJsonPredicateHints: Boolean,
    // Whether to evaluate user provided `jsonPredicateHints` with V2 evaluator.
    @BeanProperty var evaluateJsonPredicateHintsV2: Boolean,
    // The timeout of an incoming web request in seconds. Set to 0 for no timeout
    @BeanProperty var requestTimeoutSeconds: Long,
    // The maximum page size permitted by queryTable/queryTableChanges API.
    @BeanProperty var queryTablePageSizeLimit: Int,
    // The TTL of the page token generated in queryTable/queryTableChanges API (in milliseconds).
    @BeanProperty var queryTablePageTokenTtlMs: Int,
    // The TTL of the refresh token generated in queryTable API (in milliseconds).
    @BeanProperty var refreshTokenTtlMs: Int,
    // Whether to emit performance/timing log lines for table queries and CDF requests.
    @BeanProperty var perfLoggingEnabled: Boolean,
    // The idle connection timeout in seconds. Connections idle for longer than this are closed.
    // Should be greater than the proxy's IdleConnTimeout to avoid stale-connection EOF errors.
    // Set to 0 to use Armeria's built-in default (15 seconds).
    @BeanProperty var idleTimeoutSeconds: Long,
    // Access logging configuration for tracking share data egress via structured logs.
    @BeanProperty var accessLogging: AccessLoggingConfig,
    // The number of threads used to sign file URLs in parallel (queryTable/queryTableChanges).
    @BeanProperty var signingThreadPoolSize: Int,
    // Query performance metrics exported to Google Cloud Monitoring.
    @BeanProperty var metrics: MetricsConfig
) extends ConfigItem {
  import ServerConfig._

  def this() = {
    // Set default values here
    this(
      version = null,
      shares = Collections.emptyList(),
      authorization = null,
      ssl = null,
      host = "localhost",
      port = 80,
      endpoint = "/delta-sharing",
      preSignedUrlTimeoutSeconds = 3600,
      deltaTableCacheSize = 10,
      stalenessAcceptable = false,
      evaluatePredicateHints = false,
      evaluateJsonPredicateHints = true,
      evaluateJsonPredicateHintsV2 = true,
      requestTimeoutSeconds = 30,
      queryTablePageSizeLimit = 10000,
      queryTablePageTokenTtlMs = 259200000, // 3 days
      refreshTokenTtlMs = 3600000, // 1 hour
      perfLoggingEnabled = true,
      idleTimeoutSeconds = 120,
      accessLogging = null,
      signingThreadPoolSize = 32,
      metrics = null
    )
  }

  private def checkVersion(): Unit = {
    if (version == null) {
      throw new IllegalArgumentException("'version' must be provided")
    }
    if (version <= 0) {
      throw new IllegalArgumentException("'version' must be greater than 0")
    }
    if (version > CURRENT) {
      throw new IllegalArgumentException(s"The 'version' in the server config is $version which " +
        s"is too new. The current release supports version $CURRENT and below. " +
        s"Please upgrade to a newer release.")
    }
  }

  def save(configFile: String): Unit = {
    ServerConfig.save(this, configFile)
  }

  override def checkConfig(): Unit = {
    checkVersion()
    shares.forEach(_.checkConfig())
    if (authorization != null) {
      authorization.checkConfig()
    }
    if (ssl != null) {
      ssl.checkConfig()
    }
    if (accessLogging != null) {
      accessLogging.checkConfig()
    }
    if (metrics != null) {
      metrics.checkConfig()
    }
  }
}

/**
 * Configuration for access logging to track share data egress.
 * When enabled, structured JSON logs are emitted for each data access.
 */
case class AccessLoggingConfig(
    @BeanProperty var enabled: Boolean,
    // Header that contains the client region code (for example: US, DE).
    @BeanProperty var clientRegionHeader: String,
    // Header that contains client IP or forwarding chain (for example: X-Forwarded-For).
    @BeanProperty var clientIpHeader: String,
    // Optional mapping from region codes to pricing group labels.
    // Keys should be uppercase location codes (for example: US, DE).
    // A wildcard key "*" can be used as a catch-all default.
    // NOTE: pricingGroups is reserved for future use and not currently applied.
    @BeanProperty var pricingGroups: java.util.Map[String, String],
    // The GCP region where this server runs (for example: us-central1).
    // Used for pricing tier calculation based on source→destination pairs.
    @BeanProperty var sourceRegion: String,
    // Enable GCP traffic detection using GCP's published IP ranges (cloud.json).
    // When true, client IPs belonging to other GCP regions can be classified as inter-region
    // (cheaper) rather than internet egress. Set to false to disable this detection.
    @BeanProperty var detectGcpTraffic: Boolean,
    // GCS base path for the consolidated access log Delta table. When set, ACCESS_LOG
    // entries are written asynchronously to `{deltaTablePath}/access_log_br__system`
    // in addition to JSON logs. The Delta table must be pre-created by deltalake-admin;
    // the server does not auto-create schema. Leave null or empty to disable Delta writing.
    // Example: gs://my-bucket/datalake/data/tenant/_system
    @BeanProperty var deltaTablePath: String,
    // How often (seconds) to flush buffered access log records to the Delta table.
    @BeanProperty var deltaFlushIntervalSeconds: Int,
    // Maximum number of records to buffer before triggering an early flush.
    @BeanProperty var deltaFlushBatchSize: Int) extends ConfigItem {

  def this() = {
    this(
      enabled = false,
      clientRegionHeader = "x-client-region",
      clientIpHeader = "x-forwarded-for",
      pricingGroups = Collections.emptyMap(),
      sourceRegion = "",
      detectGcpTraffic = true,
      deltaTablePath = null,
      deltaFlushIntervalSeconds = 60,
      deltaFlushBatchSize = 1000)
  }

  override def checkConfig(): Unit = {
    // No required fields to validate
  }
}

object ServerConfig{
  /** The version that we understand */
  private val CURRENT = 1

  private def createYamlObjectMapper = {
    new ObjectMapper(new YAMLFactory)
      .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  }

  /**
   * Load the configurations for the server from the config file. If the file name ends with
   * `.yaml` or `.yml`, load it using the YAML parser. Otherwise, throw an error.
   */
  def load(configFile: String): ServerConfig = {
    if (configFile.endsWith(".yaml") || configFile.endsWith(".yml")) {
      val serverConfig =
        createYamlObjectMapper.readValue(new File(configFile), classOf[ServerConfig])
      serverConfig.checkConfig()
      serverConfig
    } else {
      throw new IOException("The server config file must be a yml or yaml file")
    }
  }

  /**
   * Serialize the [[ServerConfig]] object to the config file. If the file name ends with `.yaml`
   * or `.yml`, save it as a YAML file. Otherwise, throw an error.
   */
  def save(config: ServerConfig, configFile: String): Unit = {
    if (configFile.endsWith(".yaml") || configFile.endsWith(".yml")) {
      createYamlObjectMapper.writeValue(new File(configFile), config)
    } else {
      throw new IOException("The server config file must be a yml or yaml file")
    }
  }
}

case class Authorization(@BeanProperty var bearerToken: String) extends ConfigItem {

  def this() {
    this(null)
  }

  override def checkConfig(): Unit = {
    if (bearerToken == null) {
      throw new IllegalArgumentException("'bearerToken' in 'authorization' must be provided")
    }
  }
}

case class SSLConfig(
    @BeanProperty var selfSigned: Boolean,
    // The file of the PEM-format certificate
    @BeanProperty var certificateFile: String,
    // The file of the certificate’s private key
    @BeanProperty var certificateKeyFile: String,
    // The file storing the password to access the above certificate’s private key if it's protected
    @BeanProperty var certificatePasswordFile: String) extends ConfigItem {

  def this() {
    this(selfSigned = false, null, null, null)
  }

  override def checkConfig(): Unit = {
    if (!selfSigned) {
      if (certificateFile == null) {
        throw new IllegalArgumentException("'certificateFile' in a SSL config must be provided")
      }
      if (certificateKeyFile == null) {
        throw new IllegalArgumentException("'certificateKeyFile' in a SSL config must be provided")
      }
    }
  }
}

case class ShareConfig(
    @BeanProperty var name: String,
    @BeanProperty var schemas: java.util.List[SchemaConfig]) extends ConfigItem {

  def this() {
    this(null, Collections.emptyList())
  }

  override def checkConfig(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a share must be provided")
    }
    schemas.forEach(_.checkConfig())
  }
}

case class SchemaConfig(
    @BeanProperty var name: String,
    @BeanProperty var tables: java.util.List[TableConfig]) extends ConfigItem {

  def this() {
    this(null, Collections.emptyList())
  }

  override def checkConfig(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a schema must be provided")
    }
    tables.forEach(_.checkConfig())
  }
}

case class TableConfig(
    @BeanProperty var name: String,
    @BeanProperty var location: String,
    @BeanProperty var id: String = "",
    @BeanProperty var historyShared: Boolean = false,
  @BeanProperty var startVersion: Long = 0) extends ConfigItem {

  def this() {
    this(null, null, null)
  }

  override def checkConfig(): Unit = {
    if (name == null) {
      throw new IllegalArgumentException("'name' in a table must be provided")
    }
    if (location == null) {
      throw new IllegalArgumentException("'location' in a table must be provided")
    }
  }
}

/**
 * Configuration for query performance metrics.
 *
 * Metrics are pushed from the process to Google Cloud Monitoring: Google Managed Prometheus is
 * disabled on these GKE clusters, so there is no scraper to expose an endpoint to.
 *
 * Disabled by default on purpose. The exporter needs `roles/monitoring.metricWriter` on the
 * workload's service account; enabling it before that binding exists produces a failed API call
 * every export interval and no data. Grant the role, then set `enabled: true`.
 */
case class MetricsConfig(
    @BeanProperty var enabled: Boolean,
    // Exporter to use: "stackdriver" (Google Cloud Monitoring) or "none".
    @BeanProperty var exporter: String,
    // GCP project that owns the custom metric descriptors.
    @BeanProperty var projectId: String,
    // How often metrics are pushed. Cloud Monitoring rejects points written more than once per
    // 10 seconds for the same time series, so values below 10 are invalid.
    @BeanProperty var exportIntervalSeconds: Int,
    // Monitored resource type. `generic_task` gives every replica its own time series via
    // `task_id`; `global` would make replicas collide and lose points.
    @BeanProperty var resourceType: String,
    // `location` label: the GCP region this server runs in (for example: us-central1).
    @BeanProperty var location: String,
    // `namespace` label of the monitored resource.
    @BeanProperty var namespace: String,
    // `job` label of the monitored resource.
    @BeanProperty var job: String,
    // `task_id` label. Defaults to $POD_NAME, then the hostname; set only to override.
    @BeanProperty var taskId: String,
    // Explicit monitored-resource labels. Anything set here overrides the values derived above.
    @BeanProperty var resourceLabels: java.util.Map[String, String],
    // Whether to add a `tenant` label to request metrics. Off by default: each tenant multiplies
    // the number of billed Cloud Monitoring time series.
    @BeanProperty var tenantLabelEnabled: Boolean,
    // Tags added to every metric, for example the environment name.
    @BeanProperty var commonTags: java.util.Map[String, String]) extends ConfigItem {

  def this() = {
    this(
      enabled = false,
      exporter = "stackdriver",
      projectId = null,
      exportIntervalSeconds = 60,
      resourceType = "generic_task",
      location = null,
      namespace = "delta-sharing",
      job = "delta-sharing-server",
      taskId = null,
      resourceLabels = Collections.emptyMap(),
      tenantLabelEnabled = false,
      commonTags = Collections.emptyMap())
  }

  override def checkConfig(): Unit = {
    if (!enabled) {
      return
    }
    if (exporter != null && exporter.trim.equalsIgnoreCase("stackdriver")) {
      if (projectId == null || projectId.trim.isEmpty) {
        throw new IllegalArgumentException(
          "'projectId' in 'metrics' must be provided when the stackdriver exporter is enabled")
      }
    }
    if (exportIntervalSeconds < 10) {
      throw new IllegalArgumentException(
        "'exportIntervalSeconds' in 'metrics' must be at least 10: Cloud Monitoring rejects " +
          "points written more frequently than once per 10 seconds for the same time series")
    }
  }
}
