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

import java.io.OutputStreamWriter
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Collections
import java.util.concurrent.{Executors, ScheduledExecutorService, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.google.auth.oauth2.GoogleCredentials
import org.slf4j.LoggerFactory

import io.delta.sharing.server.common.JsonUtils
import io.delta.sharing.server.config.{EgressMetricsConfig, ServerConfig}

case class EgressMetricPoint(
    share: String,
    requestType: String,
    egressBytes: Long,
    startTimeMs: Long,
    endTimeMs: Long)

trait EgressMetricsEmitter {
  def record(point: EgressMetricPoint): Unit
}

trait MonitoringTimeSeriesClient {
  def write(timeSeries: Seq[Map[String, Any]]): Int
}

private class HttpMonitoringTimeSeriesClient(config: EgressMetricsConfig)
  extends MonitoringTimeSeriesClient {

  private val scope = Collections.singletonList("https://www.googleapis.com/auth/monitoring.write")
  private val metricWriteUrl =
    s"https://monitoring.googleapis.com/v3/projects/${config.gcpProjectId}/timeSeries"
  private var credentials: GoogleCredentials = _

  override def write(timeSeries: Seq[Map[String, Any]]): Int = {
    val body = JsonUtils.toJson(Map("timeSeries" -> timeSeries))
    val connection = new URL(metricWriteUrl).openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("POST")
      connection.setConnectTimeout(3000)
      connection.setReadTimeout(5000)
      connection.setDoOutput(true)
      connection.setRequestProperty("Authorization", s"Bearer ${accessTokenValue}")
      connection.setRequestProperty("Content-Type", "application/json; charset=utf-8")

      val writer = new OutputStreamWriter(connection.getOutputStream, UTF_8)
      try {
        writer.write(body)
      } finally {
        writer.close()
      }
      connection.getResponseCode
    } finally {
      connection.disconnect()
    }
  }

  private def accessTokenValue: String = synchronized {
    if (credentials == null) {
      credentials = GoogleCredentials.getApplicationDefault().createScoped(scope)
    }
    val token = credentials.getAccessToken
    if (token == null ||
      token.getExpirationTime == null ||
      token.getExpirationTime.getTime <= (System.currentTimeMillis() + 60000L)) {
      credentials.refresh()
    }
    credentials.getAccessToken.getTokenValue
  }
}

object EgressMetricsEmitter {
  val ShareEgressBytesMetric = "custom.googleapis.com/delta_sharing/share_egress_bytes"
  val UnknownShare = "unknown"
  val QueryRequestType = "query"
  val CdfStreamRequestType = "cdf_stream"

  def create(serverConfig: ServerConfig): EgressMetricsEmitter = {
    val cfg = Option(serverConfig.getEgressMetrics)
    cfg match {
      case Some(c) if c.enabled => new GcpCloudMonitoringEgressMetricsEmitter(c)
      case _ => NoopEgressMetricsEmitter
    }
  }
}

object NoopEgressMetricsEmitter extends EgressMetricsEmitter {
  override def record(point: EgressMetricPoint): Unit = {}
}

private case class CumulativeTotals(var bytes: Long, var seriesStartMs: Long)

private sealed trait SendResult
private case object SendSuccess extends SendResult
private case object SendRetryableFailure extends SendResult
private case class SendNonRetryableFailure(status: Int) extends SendResult

class GcpCloudMonitoringEgressMetricsEmitter(
    config: EgressMetricsConfig,
    client: MonitoringTimeSeriesClient,
    scheduler: ScheduledExecutorService,
    registerShutdownHook: Boolean,
    maxPendingSeries: Int,
    maxRetryAttempts: Int,
    retryBaseDelayMs: Long) extends EgressMetricsEmitter {

  def this(config: EgressMetricsConfig) = {
    this(
      config = config,
      client = new HttpMonitoringTimeSeriesClient(config),
      scheduler = GcpCloudMonitoringEgressMetricsEmitter.newDefaultScheduler(),
      registerShutdownHook = true,
      maxPendingSeries = math.max(config.batchSize * 200, config.batchSize),
      maxRetryAttempts = 3,
      retryBaseDelayMs = 200L
    )
  }

  import EgressMetricsEmitter._

  private val logger = LoggerFactory.getLogger(classOf[GcpCloudMonitoringEgressMetricsEmitter])
  private val cumulativeByLabel =
    scala.collection.mutable.HashMap.empty[String, CumulativeTotals]
  private val pendingSeries = ArrayBuffer.empty[Map[String, Any]]
  private val flushInFlight = new AtomicBoolean(false)
  private val random = new Random()
  private var droppedSeriesCount = 0L

  scheduler.scheduleAtFixedRate(
    new Runnable {
      override def run(): Unit = {
        triggerAsyncFlush()
      }
    },
    config.flushIntervalSeconds,
    config.flushIntervalSeconds,
    TimeUnit.SECONDS)

  if (registerShutdownHook) {
    // scalastyle:off runtimeaddshutdownhook
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        try {
          flushPendingSynchronously()
        } finally {
          scheduler.shutdownNow()
        }
      }
    }))
    // scalastyle:on runtimeaddshutdownhook
  }

  override def record(point: EgressMetricPoint): Unit = synchronized {
    val share = sanitizeLabel(point.share, UnknownShare)
    val requestType = sanitizeLabel(point.requestType, QueryRequestType)
    val key = s"$share|$requestType"
    val startMs = if (point.startTimeMs > 0) point.startTimeMs else System.currentTimeMillis()
    val totals = cumulativeByLabel.getOrElseUpdate(key, CumulativeTotals(0L, startMs))
    totals.bytes += point.egressBytes
    val endMs = math.max(point.endTimeMs, System.currentTimeMillis())

    pendingSeries += makeTimeSeries(
      ShareEgressBytesMetric,
      share,
      requestType,
      totals.seriesStartMs,
      endMs,
      Map("int64Value" -> totals.bytes.toString))

    if (pendingSeries.size > maxPendingSeries) {
      val overflow = pendingSeries.size - maxPendingSeries
      pendingSeries.remove(0, overflow)
      droppedSeriesCount += overflow
      if (droppedSeriesCount % 100 == 0) {
        logger.warn(
          s"Dropped $droppedSeriesCount queued egress metric points due to queue overload")
      }
    }

    if (pendingSeries.size >= config.batchSize) {
      triggerAsyncFlush()
    }
  }

  private def triggerAsyncFlush(): Unit = {
    if (flushInFlight.compareAndSet(false, true)) {
      scheduler.execute(new Runnable {
        override def run(): Unit = {
          try {
            flushPendingSynchronously()
          } finally {
            flushInFlight.set(false)
          }
        }
      })
    }
  }

  private[telemetry] def snapshotPendingSeriesSize(): Int = synchronized {
    pendingSeries.size
  }

  private def sanitizeLabel(value: String, fallback: String): String = {
    if (value == null || value.trim.isEmpty) fallback else value.trim
  }

  private def toRfc3339(ms: Long): String = {
    java.time.Instant.ofEpochMilli(ms).toString
  }

  private def makeTimeSeries(
      metricType: String,
      share: String,
      requestType: String,
      startTimeMs: Long,
      endTimeMs: Long,
      value: Map[String, Any]): Map[String, Any] = {
    Map(
      "metric" -> Map(
        "type" -> metricType,
        "labels" -> Map(
          "share" -> share,
          "request_type" -> requestType
        )
      ),
      "resource" -> Map(
        "type" -> "global",
        "labels" -> Map("project_id" -> config.gcpProjectId)
      ),
      "points" -> Seq(Map(
        "interval" -> Map(
          "startTime" -> toRfc3339(startTimeMs),
          "endTime" -> toRfc3339(endTimeMs)
        ),
        "value" -> value
      ))
    )
  }

  private def flushPendingSynchronously(): Unit = {
    val snapshot = synchronized {
      if (pendingSeries.isEmpty) {
        Seq.empty[Map[String, Any]]
      } else {
        val batch = pendingSeries.toVector
        pendingSeries.clear()
        batch
      }
    }
    if (snapshot.isEmpty) {
      return
    }

    val batches = snapshot.grouped(config.batchSize).toSeq
    batches.foreach { batch =>
      handleBatchWrite(batch)
    }
  }

  private def handleBatchWrite(batch: Seq[Map[String, Any]]): Unit = {
    sendWithRetries(batch) match {
      case SendSuccess =>
      case SendRetryableFailure =>
        synchronized {
          pendingSeries.prependAll(batch)
      }
      case SendNonRetryableFailure(status) if batch.size > 1 =>
        val (left, right) = batch.splitAt(batch.size / 2)
        handleBatchWrite(left)
        handleBatchWrite(right)
      case SendNonRetryableFailure(status) =>
        logger.warn(s"Dropping non-retryable egress metric point. status=$status")
    }
  }

  private def sendWithRetries(batch: Seq[Map[String, Any]]): SendResult = {
    var attempt = 1
    while (attempt <= maxRetryAttempts) {
      try {
        val status = client.write(batch)
        if (status / 100 == 2) {
          return SendSuccess
        }
        if (!isRetryableStatus(status)) {
          return SendNonRetryableFailure(status)
        }
        if (attempt < maxRetryAttempts) {
          backoffSleep(attempt)
        }
      } catch {
        case e: Throwable =>
          logger.warn(
            s"Failed to write egress metrics (attempt=$attempt/$maxRetryAttempts)",
            e)
          if (attempt < maxRetryAttempts) {
            backoffSleep(attempt)
          }
      }
      attempt += 1
    }
    SendRetryableFailure
  }

  private def isRetryableStatus(status: Int): Boolean = {
    status == 429 || status == 500 || status == 502 || status == 503 || status == 504
  }

  private def backoffSleep(attempt: Int): Unit = {
    val jitter = random.nextInt(100)
    val delay = retryBaseDelayMs * (1L << (attempt - 1)) + jitter
    Thread.sleep(delay)
  }
}

object GcpCloudMonitoringEgressMetricsEmitter {
  private[telemetry] def newDefaultScheduler(): ScheduledExecutorService = {
    Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, "delta-sharing-egress-metrics")
        t.setDaemon(true)
        t
      }
    })
  }
}
