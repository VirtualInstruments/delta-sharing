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

import java.util.concurrent.{Executors, ScheduledExecutorService}

import org.scalatest.FunSuite

import io.delta.sharing.server.config.EgressMetricsConfig

class EgressMetricsEmitterSuite extends FunSuite {

  private class FakeClient(sleepMs: Long = 0L) extends MonitoringTimeSeriesClient {
    val writes = scala.collection.mutable.ArrayBuffer.empty[Seq[Map[String, Any]]]

    override def write(timeSeries: Seq[Map[String, Any]]): MonitoringWriteResult = {
      if (sleepMs > 0) {
        Thread.sleep(sleepMs)
      }
      writes.synchronized {
        writes += timeSeries
      }
      MonitoringWriteResult(200)
    }

    def writeCount: Int = writes.synchronized {
      writes.size
    }
  }

  private def mkConfig(batchSize: Int, flushIntervalSeconds: Int): EgressMetricsConfig = {
    val cfg = new EgressMetricsConfig()
    cfg.setEnabled(true)
    cfg.setGcpProjectId("test-project")
    cfg.setBatchSize(batchSize)
    cfg.setFlushIntervalSeconds(flushIntervalSeconds)
    cfg.setCdfAggregationWindowSeconds(60)
    cfg
  }

  private def waitUntil(condition: => Boolean, timeoutMs: Long): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (!condition && System.currentTimeMillis() < deadline) {
      Thread.sleep(20)
    }
    assert(condition)
  }

  private def newEmitter(
      config: EgressMetricsConfig,
      client: MonitoringTimeSeriesClient,
      scheduler: ScheduledExecutorService): GcpCloudMonitoringEgressMetricsEmitter = {
    new GcpCloudMonitoringEgressMetricsEmitter(
      config = config,
      client = client,
      scheduler = scheduler,
      registerShutdownHook = false,
      maxPendingSeries = 1000,
      maxRetryAttempts = 2,
      retryBaseDelayMs = 10L)
  }

  private def extractIntervals(client: FakeClient): Seq[(String, String)] = {
    client.writes.synchronized {
      client.writes.flatMap { batch =>
        batch.map { timeSeries =>
          val points = timeSeries("points").asInstanceOf[Seq[Map[String, Any]]]
          val interval = points.head("interval").asInstanceOf[Map[String, Any]]
          (
            interval("startTime").asInstanceOf[String],
            interval("endTime").asInstanceOf[String]
          )
        }
      }.toSeq
    }
  }

  test("gauge metric points use equal start and end times") {
    val scheduler = Executors.newSingleThreadScheduledExecutor()
    try {
      val client = new FakeClient()
      val emitter = newEmitter(mkConfig(batchSize = 1, flushIntervalSeconds = 60), client, scheduler)

      emitter.record(EgressMetricPoint("share-a", "query", 10L, 1000L, 2000L))
      emitter.record(EgressMetricPoint("share-a", "query", 20L, 7000L, 8000L))

      waitUntil(client.writeCount >= 2, timeoutMs = 3000)
      val intervals = extractIntervals(client)
      assert(intervals.nonEmpty)
      assert(intervals.forall { case (start, end) => start == end })
    } finally {
      scheduler.shutdownNow()
    }
  }

  test("low traffic still flushes via scheduled flush") {
    val scheduler = Executors.newSingleThreadScheduledExecutor()
    try {
      val client = new FakeClient()
      val emitter = newEmitter(mkConfig(batchSize = 10, flushIntervalSeconds = 1), client, scheduler)

      emitter.record(EgressMetricPoint("share-a", "query", 10L, 1000L, 2000L))

      waitUntil(client.writeCount >= 1, timeoutMs = 4000)
    } finally {
      scheduler.shutdownNow()
    }
  }

  test("record path is non-blocking even when monitoring write is slow") {
    val scheduler = Executors.newSingleThreadScheduledExecutor()
    try {
      val slowClient = new FakeClient(sleepMs = 500)
      val emitter = newEmitter(
        mkConfig(batchSize = 1, flushIntervalSeconds = 60),
        slowClient,
        scheduler)

      val startNs = System.nanoTime()
      emitter.record(EgressMetricPoint("share-a", "query", 10L, 1000L, 2000L))
      val elapsedMs = (System.nanoTime() - startNs) / 1000000L

      assert(elapsedMs < 250L)
      waitUntil(slowClient.writeCount >= 1, timeoutMs = 4000)
    } finally {
      scheduler.shutdownNow()
    }
  }
}
