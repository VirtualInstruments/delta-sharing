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

import java.net.InetAddress
import java.time.Duration
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.micrometer.core.instrument.{Clock, MeterRegistry}
import io.micrometer.stackdriver.{StackdriverConfig, StackdriverMeterRegistry}
import org.slf4j.LoggerFactory

import io.delta.sharing.server.config.{MetricsConfig, ServerConfig}

/**
 * Builds the [[QueryMetrics]] implementation described by the server config.
 *
 * The only exporter wired up today is Google Cloud Monitoring (Stackdriver), which is where this
 * deployment's metrics live: Google Managed Prometheus is disabled on these GKE clusters
 * (`managed_prometheus { enabled = false }` in the cluster Terraform), so an in-process push to
 * the Cloud Monitoring API is the path that actually reaches a dashboard.
 *
 * Metrics land under `custom.googleapis.com/delta_sharing/...`.
 */
object MetricsRegistries {
  private val logger = LoggerFactory.getLogger(MetricsRegistries.getClass)

  val ExporterStackdriver = "stackdriver"
  val ExporterNone = "none"

  /**
   * Create the metrics recorder for this server.
   *
   * Never throws: if the exporter cannot be constructed (missing credentials, unreachable API,
   * bad config) the server starts with metrics disabled rather than refusing to serve queries.
   */
  def create(serverConfig: ServerConfig): QueryMetrics = {
    val config = Option(serverConfig.getMetrics)
    config match {
      case Some(c) if c.enabled =>
        buildRegistry(c) match {
          case Some(registry) => new MicrometerQueryMetrics(registry, c)
          case None => NoopQueryMetrics
        }
      case _ =>
        logger.info("Query metrics are disabled")
        NoopQueryMetrics
    }
  }

  private def buildRegistry(config: MetricsConfig): Option[MeterRegistry] = {
    Option(config.getExporter).map(_.trim.toLowerCase(java.util.Locale.ROOT)) match {
      case Some(ExporterStackdriver) => buildStackdriverRegistry(config)
      case Some(ExporterNone) | None =>
        logger.info("Query metrics enabled but no exporter configured; metrics will not be sent")
        None
      case Some(other) =>
        logger.warn("Unknown metrics exporter '{}'; metrics will not be sent", other)
        None
    }
  }

  private def buildStackdriverRegistry(config: MetricsConfig): Option[MeterRegistry] = {
    try {
      val registry = StackdriverMeterRegistry
        .builder(stackdriverConfig(config))
        .clock(Clock.SYSTEM)
        .build()
      logger.info(
        "Publishing metrics to Google Cloud Monitoring: project={} resourceType={} taskId={} " +
          "intervalSeconds={}",
        config.getProjectId,
        config.getResourceType,
        resolvedTaskId(config),
        config.getExportIntervalSeconds.toString)
      Some(registry)
    } catch {
      case NonFatal(e) =>
        // Losing metrics must never take the server down with it.
        logger.error("Failed to start the Cloud Monitoring exporter; metrics are disabled", e)
        None
    }
  }

  private[telemetry] def stackdriverConfig(config: MetricsConfig): StackdriverConfig = {
    val labels = resourceLabels(config)
    new StackdriverConfig {
      override def projectId(): String = config.getProjectId
      override def resourceType(): String = config.getResourceType
      override def resourceLabels(): java.util.Map[String, String] = labels
      override def step(): Duration = Duration.ofSeconds(config.getExportIntervalSeconds.toLong)
      // Every setting comes from the server config, so there is no external property source.
      override def get(key: String): String = null
    }
  }

  /**
   * Labels identifying the writer of each time series.
   *
   * `generic_task` with a per-pod `task_id` is deliberate. Under the `global` resource type every
   * replica writes to the same time series, and Cloud Monitoring rejects points written more than
   * once per sampling period -- with an HPA in front of this deployment that would silently drop
   * most of the data.
   */
  private[telemetry] def resourceLabels(config: MetricsConfig): java.util.Map[String, String] = {
    val declared = Option(config.getResourceLabels)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    val defaults = Map(
      "project_id" -> Option(config.getProjectId).getOrElse(""),
      "location" -> Option(config.getLocation).filter(_.nonEmpty).getOrElse("global"),
      "namespace" -> Option(config.getNamespace).filter(_.nonEmpty).getOrElse("delta-sharing"),
      "job" -> Option(config.getJob).filter(_.nonEmpty).getOrElse("delta-sharing-server"),
      "task_id" -> resolvedTaskId(config))
    // Explicit labels win, so an operator can override any of the defaults from the config file.
    (defaults ++ declared).asJava
  }

  /**
   * Stable per-pod identity: the configured value, else `$POD_NAME` from the downward API, else
   * the hostname, else a random id so that two replicas never share a time series.
   */
  private[telemetry] def resolvedTaskId(config: MetricsConfig): String = {
    Option(config.getTaskId).map(_.trim).filter(_.nonEmpty)
      .orElse(sys.env.get("POD_NAME").map(_.trim).filter(_.nonEmpty))
      .orElse(hostname)
      .getOrElse(s"delta-sharing-${UUID.randomUUID().toString.take(8)}")
  }

  private def hostname: Option[String] = {
    try {
      Option(InetAddress.getLocalHost.getHostName).map(_.trim).filter(_.nonEmpty)
    } catch {
      case NonFatal(_) => None
    }
  }
}
