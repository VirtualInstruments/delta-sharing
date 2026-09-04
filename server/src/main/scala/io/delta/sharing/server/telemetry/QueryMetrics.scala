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

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import io.micrometer.core.instrument.{Counter, DistributionSummary, Gauge, MeterRegistry, Tag, Tags, Timer}
import org.slf4j.LoggerFactory

import io.delta.sharing.server.{CdfTimings, QueryResultTimings, TableTimings}
import io.delta.sharing.server.config.MetricsConfig

/**
 * Stage names used for the `stage` label on `delta_sharing.query.stage.duration`.
 *
 * `Unattributed` is the residual (`total - sum(stages)`). It is recorded deliberately: without it,
 * drift between wall time and the sum of the measured stages goes unnoticed, and a dashboard that
 * stacks the stages silently under-reports.
 */
object QueryStage {
  val DeltaLogUpdate = "delta_log_update"
  val SnapshotResolve = "snapshot_resolve"
  val ProtocolSnapshot = "protocol_snapshot"
  val TimestampIndex = "timestamp_index"
  val ReplayOrPrepare = "replay_or_prepare"
  val GetChanges = "get_changes"
  val CdcSpecBuild = "cdc_spec_build"
  val Signing = "signing"
  val ResponseBuild = "response_build"
  val Unattributed = "unattributed"
}

/** Outcome label values, derived from the response status and cause. */
object RequestOutcome {
  val Ok = "ok"
  val ClientError = "client_error"
  val ServerError = "server_error"
  val Timeout = "timeout"
  val ClientDisconnect = "client_disconnect"
  val Unknown = "unknown"
}

/**
 * Collection point for query performance metrics.
 *
 * Everything the server measures funnels through this one interface so that timing instrumentation
 * stays out of the query paths: the stage boundaries already exist as [[QueryResultTimings]], and
 * this trait turns them into metrics. Implementations must be cheap and must never throw -- a
 * telemetry failure may not fail a customer query.
 */
trait QueryMetrics {

  /** Called when a request enters the server. Pairs with [[requestFinished]]. */
  def requestStarted(endpoint: String): Unit

  // scalastyle:off argcount
  /**
   * Called once per request, after the response has completed.
   *
   * @param endpoint matched route pattern, never a raw path (cardinality)
   * @param queryClass see [[QueryClass]]
   * @param outcome see [[RequestOutcome]]
   * @param statusClass `2xx`, `4xx`, `5xx` or `unknown`
   * @param tenant tenant id, recorded only when `tenantLabelEnabled` is set
   * @param durationNs total wall time
   * @param ttfbNs time to first response byte, when known
   * @param responseBytes serialized response size
   * @param nearTimeout whether the request exceeded 75% of `requestTimeoutSeconds`
   */
  def requestFinished(
      endpoint: String,
      queryClass: String,
      outcome: String,
      statusClass: String,
      tenant: Option[String],
      durationNs: Long,
      ttfbNs: Option[Long],
      responseBytes: Long,
      nearTimeout: Boolean): Unit
  // scalastyle:on argcount

  /**
   * Called once per completed table query or CDF request, with the stage breakdown.
   *
   * @param queryClass see [[QueryClass]]
   * @param engine `standalone` or `kernel`
   * @param totalNs wall time of the query, used to derive the unattributed residual
   * @param signedUrls number of pre-signed URLs in the response, when the path tracked it
   * @param timings stage boundaries captured by the query path, when available
   */
  def queryCompleted(
      queryClass: String,
      engine: String,
      totalNs: Long,
      signedUrls: Option[Int],
      timings: Option[QueryResultTimings]): Unit

  /** Flush and release the exporter. */
  def close(): Unit = {}
}

/** Used when metrics are disabled. Every call is a no-op. */
object NoopQueryMetrics extends QueryMetrics {
  override def requestStarted(endpoint: String): Unit = {}

  // scalastyle:off argcount
  override def requestFinished(
      endpoint: String,
      queryClass: String,
      outcome: String,
      statusClass: String,
      tenant: Option[String],
      durationNs: Long,
      ttfbNs: Option[Long],
      responseBytes: Long,
      nearTimeout: Boolean): Unit = {}
  // scalastyle:on argcount

  override def queryCompleted(
      queryClass: String,
      engine: String,
      totalNs: Long,
      signedUrls: Option[Int],
      timings: Option[QueryResultTimings]): Unit = {}
}

/**
 * Micrometer-backed implementation.
 *
 * Meters are looked up per call; Micrometer caches by name+tags, so this is a hash lookup rather
 * than an allocation. Latency distributions carry explicit bucket boundaries (see
 * [[MicrometerQueryMetrics.LatencyBuckets]]) so that "fraction of requests near the timeout" is a
 * bucket ratio rather than an interpolated quantile.
 */
class MicrometerQueryMetrics(
    registry: MeterRegistry,
    config: MetricsConfig) extends QueryMetrics {

  import MicrometerQueryMetrics._

  private val inFlight = new ConcurrentHashMap[String, AtomicInteger]()

  /** Logged once; a registry that fails tends to fail on every subsequent call. */
  private val reportedFailure = new AtomicBoolean(false)

  /**
   * Runs a recording block, swallowing any failure.
   *
   * The trait documents a no-throw contract, and it has to be enforced here rather than assumed:
   * `requestStarted` runs before the request is served and `queryCompleted` before the response
   * is returned, so an exception escaping either one would turn a telemetry problem into a failed
   * customer query.
   */
  private def guarded(operation: String)(body: => Unit): Unit = {
    try {
      body
    } catch {
      case NonFatal(e) =>
        if (reportedFailure.compareAndSet(false, true)) {
          MicrometerQueryMetrics.logger.warn(
            s"Recording query metrics failed ($operation); metrics may be incomplete. " +
              "Further failures are not logged.", e)
        }
    }
  }

  private val commonTags: Tags = {
    val extra = Option(config.getCommonTags)
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
      .collect { case (k, v) if k != null && v != null => Tag.of(k, v) }
    Tags.of(extra.asJava)
  }

  override def requestStarted(endpoint: String): Unit = guarded("requestStarted") {
    inFlightCounter(endpoint).incrementAndGet()
  }

  // scalastyle:off argcount
  override def requestFinished(
      endpoint: String,
      queryClass: String,
      outcome: String,
      statusClass: String,
      tenant: Option[String],
      durationNs: Long,
      ttfbNs: Option[Long],
      responseBytes: Long,
      nearTimeout: Boolean): Unit = guarded("requestFinished") {
    inFlightCounter(endpoint).decrementAndGet()

    val base = commonTags.and("endpoint", endpoint).and("query_class", queryClass)
    val withTenant = if (config.tenantLabelEnabled) {
      base.and("tenant", tenant.getOrElse("unknown"))
    } else {
      base
    }

    Timer.builder(RequestDuration)
      .tags(withTenant.and("outcome", outcome))
      .serviceLevelObjectives(LatencyBuckets: _*)
      .publishPercentileHistogram()
      .register(registry)
      .record(durationNs, java.util.concurrent.TimeUnit.NANOSECONDS)

    Counter.builder(RequestCount)
      .tags(withTenant.and("status_class", statusClass))
      .register(registry)
      .increment()

    if (responseBytes > 0) {
      DistributionSummary.builder(ResponseBytes)
        .tags(base)
        .baseUnit("bytes")
        .publishPercentileHistogram()
        .register(registry)
        .record(responseBytes.toDouble)
    }

    ttfbNs.foreach { ns =>
      Timer.builder(TimeToFirstByte)
        .tags(base)
        .serviceLevelObjectives(LatencyBuckets: _*)
        .publishPercentileHistogram()
        .register(registry)
        .record(ns, java.util.concurrent.TimeUnit.NANOSECONDS)
    }

    if (outcome == RequestOutcome.Timeout) {
      Counter.builder(RequestTimeouts).tags(base).register(registry).increment()
    }
    if (nearTimeout) {
      Counter.builder(RequestNearTimeouts).tags(base).register(registry).increment()
    }
  }
  // scalastyle:on argcount

  override def queryCompleted(
      queryClass: String,
      engine: String,
      totalNs: Long,
      signedUrls: Option[Int],
      timings: Option[QueryResultTimings]): Unit = guarded("queryCompleted") {
    val base = commonTags.and("query_class", queryClass)

    signedUrls.filter(_ >= 0).foreach { signed =>
      DistributionSummary.builder(FilesSigned)
        .tags(base)
        .publishPercentileHistogram()
        .register(registry)
        .record(signed.toDouble)
    }

    val stages = stageBreakdown(timings)
    if (stages.nonEmpty) {
      val stageTags = base.and("engine", engine)
      var attributed = 0L
      stages.foreach { case (stage, ns) =>
        attributed += ns
        recordStage(stageTags, stage, ns)
      }
      // The residual keeps the stacked view honest; see QueryStage.Unattributed.
      recordStage(stageTags, QueryStage.Unattributed, math.max(0L, totalNs - attributed))
    }

    versionsIterated(timings).foreach { versions =>
      DistributionSummary.builder(VersionsIterated)
        .tags(base)
        .publishPercentileHistogram()
        .register(registry)
        .record(versions.toDouble)
    }
  }

  override def close(): Unit = guarded("close") {
    registry.close()
  }

  private def recordStage(tags: Tags, stage: String, ns: Long): Unit = {
    Timer.builder(StageDuration)
      .tags(tags.and("stage", stage))
      .serviceLevelObjectives(LatencyBuckets: _*)
      .publishPercentileHistogram()
      .register(registry)
      .record(ns, java.util.concurrent.TimeUnit.NANOSECONDS)
  }

  private def inFlightCounter(endpoint: String): AtomicInteger = {
    inFlight.computeIfAbsent(endpoint, { _ =>
      val counter = new AtomicInteger(0)
      Gauge.builder(RequestsInFlight, counter, (c: AtomicInteger) => c.get().toDouble)
        .tags(commonTags.and("endpoint", endpoint))
        .register(registry)
      counter
    })
  }
}

object MicrometerQueryMetrics {
  private val logger = LoggerFactory.getLogger(classOf[MicrometerQueryMetrics])

  val RequestDuration = "delta_sharing.request.duration"
  val RequestCount = "delta_sharing.requests"
  val RequestsInFlight = "delta_sharing.requests.in_flight"
  val RequestTimeouts = "delta_sharing.request.timeouts"
  val RequestNearTimeouts = "delta_sharing.request.near_timeouts"
  val ResponseBytes = "delta_sharing.response.bytes"
  val TimeToFirstByte = "delta_sharing.request.time_to_first_byte"
  val StageDuration = "delta_sharing.query.stage.duration"
  val FilesSigned = "delta_sharing.query.files_signed"
  val VersionsIterated = "delta_sharing.query.versions_iterated"

  /**
   * Explicit latency bucket boundaries, spanning a `version` poll to a near-timeout CDF read.
   *
   * 120s and 300s are deliberate: they are `idleTimeoutSeconds` and `requestTimeoutSeconds`, so a
   * bucket edge sits exactly at each client-visible limit.
   */
  val LatencyBuckets: Seq[Duration] = Seq(
    Duration.ofMillis(5),
    Duration.ofMillis(10),
    Duration.ofMillis(25),
    Duration.ofMillis(50),
    Duration.ofMillis(100),
    Duration.ofMillis(250),
    Duration.ofMillis(500),
    Duration.ofSeconds(1),
    Duration.ofMillis(2500),
    Duration.ofSeconds(5),
    Duration.ofSeconds(10),
    Duration.ofSeconds(30),
    Duration.ofSeconds(60),
    Duration.ofSeconds(120),
    Duration.ofSeconds(300))

  /**
   * Flatten the timings captured by a query path into `(stage, nanos)` pairs.
   *
   * Returns empty when the path reported no timings -- the Kernel path does not populate them yet
   * (gap G2 in memory-bank/08-query-performance-metrics.md), and an empty breakdown is preferable
   * to a fabricated one.
   */
  def stageBreakdown(timings: Option[QueryResultTimings]): Seq[(String, Long)] = timings match {
    case Some(TableTimings(t)) =>
      Seq(
        QueryStage.DeltaLogUpdate -> t.deltaLogUpdateNs,
        QueryStage.SnapshotResolve -> t.snapshotResolveNs,
        QueryStage.ReplayOrPrepare -> t.replayOrPrepareNs,
        QueryStage.Signing -> t.signingNs)
    case Some(CdfTimings(t)) =>
      Seq(
        QueryStage.DeltaLogUpdate -> t.deltaLogUpdateNs,
        QueryStage.ProtocolSnapshot -> t.protocolSnapshotNs,
        QueryStage.TimestampIndex -> t.timestampIndexNs,
        QueryStage.GetChanges -> t.getChangesNs,
        QueryStage.CdcSpecBuild -> t.cdcSpecBuildNs,
        QueryStage.Signing -> t.signingNs,
        QueryStage.ResponseBuild -> t.responseBuildNs)
    case _ => Seq.empty
  }

  /** Number of commit versions replayed, when the query path tracked it. */
  def versionsIterated(timings: Option[QueryResultTimings]): Option[Int] = timings match {
    case Some(TableTimings(t)) => t.versionsIterated
    case Some(CdfTimings(t)) => Some(t.versionsIterated)
    case _ => None
  }
}
