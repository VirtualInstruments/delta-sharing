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

import org.scalatest.FunSuite

import io.delta.sharing.server.config.{MetricsConfig, ServerConfig}

class MetricsRegistriesSuite extends FunSuite {

  private def enabledConfig(): MetricsConfig = {
    val c = new MetricsConfig()
    c.setEnabled(true)
    c.setProjectId("test-project")
    c.setLocation("us-central1")
    c
  }

  private def serverConfigWith(metrics: MetricsConfig): ServerConfig = {
    val s = new ServerConfig()
    s.setVersion(1)
    s.setMetrics(metrics)
    s
  }

  test("metrics are a no-op when the config block is absent") {
    assert(MetricsRegistries.create(serverConfigWith(null)) eq NoopQueryMetrics)
  }

  test("metrics are a no-op when disabled") {
    val c = enabledConfig()
    c.setEnabled(false)
    assert(MetricsRegistries.create(serverConfigWith(c)) eq NoopQueryMetrics)
  }

  test("metrics are a no-op when the exporter is none or unknown") {
    val none = enabledConfig()
    none.setExporter("none")
    assert(MetricsRegistries.create(serverConfigWith(none)) eq NoopQueryMetrics)

    val unknown = enabledConfig()
    unknown.setExporter("carrier-pigeon")
    assert(MetricsRegistries.create(serverConfigWith(unknown)) eq NoopQueryMetrics)
  }

  test("resource labels identify the project, location and task") {
    val labels = MetricsRegistries.resourceLabels(enabledConfig()).asScala
    assert(labels("project_id") == "test-project")
    assert(labels("location") == "us-central1")
    assert(labels("namespace") == "delta-sharing")
    assert(labels("job") == "delta-sharing-server")
    assert(labels("task_id").nonEmpty)
  }

  test("every replica gets a distinct task id") {
    // Cloud Monitoring rejects two writers sharing a time series, so this label is what keeps
    // an HPA-scaled deployment from losing most of its points.
    val c = enabledConfig()
    c.setTaskId("pod-abc-123")
    assert(MetricsRegistries.resourceLabels(c).asScala("task_id") == "pod-abc-123")
    assert(MetricsRegistries.resolvedTaskId(c) == "pod-abc-123")
  }

  test("task id falls back to a non-empty value when nothing is configured") {
    val c = enabledConfig()
    c.setTaskId("   ")
    assert(MetricsRegistries.resolvedTaskId(c).trim.nonEmpty)
  }

  test("an empty location falls back rather than emitting an empty label") {
    val c = enabledConfig()
    c.setLocation("")
    assert(MetricsRegistries.resourceLabels(c).asScala("location") == "global")
  }

  test("explicit resource labels override the derived ones") {
    val c = enabledConfig()
    c.setResourceLabels(Map("location" -> "europe-west3", "extra" -> "value").asJava)
    val labels = MetricsRegistries.resourceLabels(c).asScala
    assert(labels("location") == "europe-west3")
    assert(labels("extra") == "value")
    assert(labels("project_id") == "test-project")
  }

  test("the stackdriver config carries the configured project, resource and interval") {
    val c = enabledConfig()
    c.setExportIntervalSeconds(30)
    val sd = MetricsRegistries.stackdriverConfig(c)
    assert(sd.projectId() == "test-project")
    assert(sd.resourceType() == "generic_task")
    assert(sd.step().getSeconds == 30L)
    assert(sd.resourceLabels().get("project_id") == "test-project")
    // No external property source: every setting comes from the server config file.
    assert(sd.get("anything") == null)
  }
}
