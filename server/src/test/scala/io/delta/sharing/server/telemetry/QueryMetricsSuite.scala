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

import scala.collection.JavaConverters._

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.scalatest.FunSuite

import io.delta.sharing.server.{CdfQueryTimings, CdfTimings, TableQueryTimings, TableTimings}
import io.delta.sharing.server.config.MetricsConfig

class QueryMetricsSuite extends FunSuite {

  private def tableTimings(
      updateNs: Long = 1000000L,
      resolveNs: Long = 2000000L,
      replayNs: Long = 3000000L,
      signingNs: Long = 4000000L,
      versions: Option[Int] = None): TableTimings = {
    TableTimings(
      TableQueryTimings(
        deltaLogUpdateNs = updateNs,
        snapshotResolveNs = resolveNs,
        replayOrPrepareNs = replayNs,
        signingNs = signingNs,
        versionsIterated = versions,
        queryStartVersion = None,
        queryEndVersion = None))
  }

  private def cdfTimings(): CdfTimings = {
    CdfTimings(
      CdfQueryTimings(
        cdfStartVersion = 10L,
        cdfEndVersion = 20L,
        versionsIterated = 11,
        deltaLogUpdateNs = 1000000L,
        protocolSnapshotNs = 2000000L,
        getChangesNs = 3000000L,
        timestampIndexNs = 4000000L,
        cdcSpecBuildNs = 5000000L,
        signingNs = 6000000L,
        responseBuildNs = 7000000L))
  }

  private def metricsConfig(tenantLabel: Boolean = false): MetricsConfig = {
    val c = new MetricsConfig()
    c.setEnabled(true)
    c.setProjectId("test-project")
    c.setTenantLabelEnabled(tenantLabel)
    c
  }

  test("stage breakdown maps table timings to the standalone query stages") {
    val stages = MicrometerQueryMetrics.stageBreakdown(Some(tableTimings())).toMap
    assert(stages(QueryStage.DeltaLogUpdate) == 1000000L)
    assert(stages(QueryStage.SnapshotResolve) == 2000000L)
    assert(stages(QueryStage.ReplayOrPrepare) == 3000000L)
    assert(stages(QueryStage.Signing) == 4000000L)
    assert(!stages.contains(QueryStage.TimestampIndex))
  }

  test("stage breakdown maps cdf timings to the cdf stages") {
    val stages = MicrometerQueryMetrics.stageBreakdown(Some(cdfTimings())).toMap
    assert(stages(QueryStage.ProtocolSnapshot) == 2000000L)
    assert(stages(QueryStage.GetChanges) == 3000000L)
    assert(stages(QueryStage.TimestampIndex) == 4000000L)
    assert(stages(QueryStage.CdcSpecBuild) == 5000000L)
    assert(stages(QueryStage.Signing) == 6000000L)
    assert(stages(QueryStage.ResponseBuild) == 7000000L)
  }

  test("stage breakdown is empty when the query path reported no timings") {
    // The kernel path does not populate timings yet; an empty breakdown beats a fabricated one.
    assert(MicrometerQueryMetrics.stageBreakdown(None).isEmpty)
    assert(MicrometerQueryMetrics.versionsIterated(None).isEmpty)
  }

  test("versions iterated is read from either timing shape") {
    assert(MicrometerQueryMetrics.versionsIterated(Some(cdfTimings())).contains(11))
    assert(
      MicrometerQueryMetrics.versionsIterated(Some(tableTimings(versions = Some(7)))).contains(7))
    assert(MicrometerQueryMetrics.versionsIterated(Some(tableTimings())).isEmpty)
  }

  test("queryCompleted records a stage timer per stage plus an unattributed residual") {
    val registry = new SimpleMeterRegistry()
    val metrics = new MicrometerQueryMetrics(registry, metricsConfig())

    // Stages sum to 10ms; total is 15ms, so 5ms is unattributed.
    metrics.queryCompleted(
      queryClass = QueryClass.Incremental,
      engine = QueryClass.EngineStandalone,
      totalNs = 15000000L,
      signedUrls = 3,
      timings = Some(tableTimings()))

    val stageTimers = registry.find(MicrometerQueryMetrics.StageDuration).timers().asScala
    val byStage = stageTimers.map(t => t.getId.getTag("stage") -> t).toMap
    assert(byStage.keySet == Set(
      QueryStage.DeltaLogUpdate,
      QueryStage.SnapshotResolve,
      QueryStage.ReplayOrPrepare,
      QueryStage.Signing,
      QueryStage.Unattributed))
    assert(byStage(QueryStage.Unattributed).totalTime(java.util.concurrent.TimeUnit.NANOSECONDS)
      == 5000000L)
    assert(byStage(QueryStage.Signing).getId.getTag("engine") == QueryClass.EngineStandalone)
    assert(byStage(QueryStage.Signing).getId.getTag("query_class") == QueryClass.Incremental)
  }

  test("unattributed residual never goes negative when stages exceed the measured total") {
    val registry = new SimpleMeterRegistry()
    val metrics = new MicrometerQueryMetrics(registry, metricsConfig())

    // Stage sums can exceed wall time: signing time is summed across the parallel signing pool.
    metrics.queryCompleted(
      queryClass = QueryClass.Snapshot,
      engine = QueryClass.EngineStandalone,
      totalNs = 1000000L,
      signedUrls = 1,
      timings = Some(tableTimings()))

    val residual = registry.find(MicrometerQueryMetrics.StageDuration)
      .tag("stage", QueryStage.Unattributed)
      .timer()
    assert(residual.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS) == 0L)
  }

  test("queryCompleted records work volume") {
    val registry = new SimpleMeterRegistry()
    val metrics = new MicrometerQueryMetrics(registry, metricsConfig())

    metrics.queryCompleted(
      queryClass = QueryClass.Cdf,
      engine = QueryClass.EngineStandalone,
      totalNs = 50000000L,
      signedUrls = 42,
      timings = Some(cdfTimings()))

    val signed = registry.find(MicrometerQueryMetrics.FilesSigned).summary()
    assert(signed.totalAmount() == 42.0)
    val versions = registry.find(MicrometerQueryMetrics.VersionsIterated).summary()
    assert(versions.totalAmount() == 11.0)
  }

  test("requestFinished records duration, count and in-flight") {
    val registry = new SimpleMeterRegistry()
    val metrics = new MicrometerQueryMetrics(registry, metricsConfig())

    metrics.requestStarted("/query")
    val inFlight = registry.find(MicrometerQueryMetrics.RequestsInFlight).gauge()
    assert(inFlight.value() == 1.0)

    metrics.requestFinished(
      endpoint = "/query",
      queryClass = QueryClass.Snapshot,
      outcome = RequestOutcome.Ok,
      statusClass = "2xx",
      tenant = Some("tenant-a"),
      durationNs = 2000000L,
      ttfbNs = Some(1000000L),
      responseBytes = 4096L,
      nearTimeout = false)

    assert(inFlight.value() == 0.0)
    val timer = registry.find(MicrometerQueryMetrics.RequestDuration).timer()
    assert(timer.count() == 1L)
    assert(timer.getId.getTag("outcome") == RequestOutcome.Ok)
    assert(registry.find(MicrometerQueryMetrics.RequestCount).counter().count() == 1.0)
    assert(registry.find(MicrometerQueryMetrics.ResponseBytes).summary().totalAmount() == 4096.0)
    assert(registry.find(MicrometerQueryMetrics.TimeToFirstByte).timer().count() == 1L)
  }

  test("tenant label is only attached when explicitly enabled") {
    val off = new SimpleMeterRegistry()
    new MicrometerQueryMetrics(off, metricsConfig(tenantLabel = false)).requestFinished(
      "/query", QueryClass.Snapshot, RequestOutcome.Ok, "2xx", Some("tenant-a"),
      1000L, None, 0L, nearTimeout = false)
    assert(off.find(MicrometerQueryMetrics.RequestDuration).timer().getId.getTag("tenant") == null)

    val on = new SimpleMeterRegistry()
    new MicrometerQueryMetrics(on, metricsConfig(tenantLabel = true)).requestFinished(
      "/query", QueryClass.Snapshot, RequestOutcome.Ok, "2xx", Some("tenant-a"),
      1000L, None, 0L, nearTimeout = false)
    assert(
      on.find(MicrometerQueryMetrics.RequestDuration).timer().getId.getTag("tenant") == "tenant-a")
  }

  test("timeout and near-timeout counters only fire when the condition holds") {
    val registry = new SimpleMeterRegistry()
    val metrics = new MicrometerQueryMetrics(registry, metricsConfig())

    metrics.requestFinished(
      "/query", QueryClass.Cdf, RequestOutcome.Ok, "2xx", None, 1000L, None, 0L,
      nearTimeout = false)
    assert(registry.find(MicrometerQueryMetrics.RequestTimeouts).counter() == null)
    assert(registry.find(MicrometerQueryMetrics.RequestNearTimeouts).counter() == null)

    metrics.requestFinished(
      "/query", QueryClass.Cdf, RequestOutcome.Timeout, "5xx", None, 1000L, None, 0L,
      nearTimeout = true)
    assert(registry.find(MicrometerQueryMetrics.RequestTimeouts).counter().count() == 1.0)
    assert(registry.find(MicrometerQueryMetrics.RequestNearTimeouts).counter().count() == 1.0)
  }

  test("common tags are applied to every metric") {
    val registry = new SimpleMeterRegistry()
    val config = metricsConfig()
    config.setCommonTags(Map("environment" -> "zing-dev").asJava)
    val metrics = new MicrometerQueryMetrics(registry, config)

    metrics.queryCompleted(
      QueryClass.Snapshot, QueryClass.EngineKernel, 1000000L, 1, Some(tableTimings()))

    val timer = registry.find(MicrometerQueryMetrics.StageDuration).timers().asScala.head
    assert(timer.getId.getTag("environment") == "zing-dev")
  }

  test("NoopQueryMetrics accepts every call without a registry") {
    NoopQueryMetrics.requestStarted("/query")
    NoopQueryMetrics.requestFinished(
      "/query", QueryClass.Snapshot, RequestOutcome.Ok, "2xx", None, 1L, Some(1L), 1L, false)
    NoopQueryMetrics.queryCompleted(QueryClass.Cdf, QueryClass.EngineStandalone, 1L, 0, None)
    NoopQueryMetrics.close()
  }
}
