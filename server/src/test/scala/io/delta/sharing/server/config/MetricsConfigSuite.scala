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

package io.delta.sharing.server.config

import org.scalatest.FunSuite

class MetricsConfigSuite extends FunSuite {

  private def config(): MetricsConfig = new MetricsConfig()

  test("metrics are disabled by default") {
    // The exporter needs roles/monitoring.metricWriter, so it must be opted into explicitly.
    assert(!config().enabled)
    assert(config().getExporter == "stackdriver")
    assert(config().getExportIntervalSeconds == 60)
    assert(config().getResourceType == "generic_task")
    assert(!config().tenantLabelEnabled)
  }

  test("a disabled config is never validated") {
    val c = config()
    c.setProjectId(null)
    c.setExportIntervalSeconds(1)
    c.checkConfig()
  }

  test("the stackdriver exporter requires a project id") {
    val c = config()
    c.setEnabled(true)
    val e = intercept[IllegalArgumentException](c.checkConfig())
    assert(e.getMessage.contains("projectId"))

    c.setProjectId("  ")
    assert(intercept[IllegalArgumentException](c.checkConfig()).getMessage.contains("projectId"))

    c.setProjectId("my-project")
    c.checkConfig()
  }

  test("the project id is not required when no exporter is configured") {
    val c = config()
    c.setEnabled(true)
    c.setExporter("none")
    c.checkConfig()
  }

  test("an export interval below the Cloud Monitoring sampling period is rejected") {
    val c = config()
    c.setEnabled(true)
    c.setProjectId("my-project")
    c.setExportIntervalSeconds(5)
    val e = intercept[IllegalArgumentException](c.checkConfig())
    assert(e.getMessage.contains("exportIntervalSeconds"))

    c.setExportIntervalSeconds(10)
    c.checkConfig()
  }

  test("ServerConfig validates a nested metrics block") {
    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    val metrics = config()
    metrics.setEnabled(true)
    serverConfig.setMetrics(metrics)
    assert(intercept[IllegalArgumentException](serverConfig.checkConfig())
      .getMessage.contains("projectId"))
  }

  test("ServerConfig defaults to no metrics block") {
    assert(new ServerConfig().getMetrics == null)
  }

  test("the metrics block deserializes from the deployed config shape") {
    // Mirrors manifests/base/configmap.yaml so a drift between the two shows up as a test failure
    // rather than as a server that silently starts with metrics disabled.
    val yaml =
      """version: 1
        |shares: []
        |metrics:
        |  enabled: true
        |  exporter: "stackdriver"
        |  projectId: "zing-dev-197522"
        |  exportIntervalSeconds: 60
        |  resourceType: "generic_task"
        |  location: "us-central1"
        |  namespace: "delta-sharing"
        |  job: "delta-sharing-server"
        |  tenantLabelEnabled: false
        |""".stripMargin

    val file = java.io.File.createTempFile("delta-sharing-metrics", ".yaml")
    try {
      val out = new java.io.PrintWriter(file, "UTF-8")
      try out.write(yaml) finally out.close()

      val loaded = ServerConfig.load(file.getCanonicalPath)
      val metrics = loaded.getMetrics
      assert(metrics != null)
      assert(metrics.enabled)
      assert(metrics.getExporter == "stackdriver")
      assert(metrics.getProjectId == "zing-dev-197522")
      assert(metrics.getExportIntervalSeconds == 60)
      assert(metrics.getResourceType == "generic_task")
      assert(metrics.getLocation == "us-central1")
      assert(metrics.getNamespace == "delta-sharing")
      assert(metrics.getJob == "delta-sharing-server")
      assert(!metrics.tenantLabelEnabled)
    } finally {
      file.delete()
    }
  }
}
