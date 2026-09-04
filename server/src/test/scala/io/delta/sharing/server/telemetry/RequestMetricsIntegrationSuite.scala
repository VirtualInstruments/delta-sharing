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

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.linecorp.armeria.client.WebClient
import com.linecorp.armeria.common.HttpStatus
import com.linecorp.armeria.common.auth.OAuth2Token
import com.linecorp.armeria.server.{HttpService, Server, ServiceRequestContext}
import com.linecorp.armeria.server.annotation.{Get, ProducesJson}
import com.linecorp.armeria.server.auth.AuthService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.scalatest.FunSuite

import io.delta.sharing.server.config.MetricsConfig

class RequestMetricsIntegrationSuite extends FunSuite {

  class TestService {
    @Get("/ok")
    @ProducesJson
    def ok(): String = "hello"

    @Get("/boom")
    @ProducesJson
    def boom(): String = throw new RuntimeException("boom")

    @Get("/labelled")
    @ProducesJson
    def labelled(): String = {
      // Mirrors what the real handlers do once they have parsed the request body.
      RequestMetrics.setQueryClass(QueryClass.Incremental)
      RequestMetrics.setTenant("tenant-xyz")
      "labelled"
    }
  }

  private def withServer(
      registry: SimpleMeterRegistry,
      tenantLabel: Boolean = false)(f: WebClient => Unit): Unit = {
    val config = new MetricsConfig()
    config.setEnabled(true)
    config.setProjectId("test-project")
    config.setTenantLabelEnabled(tenantLabel)
    val metrics = new MicrometerQueryMetrics(registry, config)

    val server = Server.builder()
      .http(0)
      .annotatedService("/test", new TestService(): Any)
      .decorator(new java.util.function.Function[HttpService, HttpService] {
        override def apply(delegate: HttpService): HttpService =
          new RequestMetricsService(delegate, metrics, 300L)
      })
      .build()
    server.start().get()
    try {
      f(WebClient.of(s"http://127.0.0.1:${server.activeLocalPort()}"))
    } finally {
      server.stop().get()
    }
  }

  test("a successful request is recorded with an endpoint, outcome and response size") {
    val registry = new SimpleMeterRegistry()
    withServer(registry) { client =>
      val response = client.get("/test/ok").aggregate().get()
      assert(response.status() == HttpStatus.OK)
    }

    val timer = registry.find(MicrometerQueryMetrics.RequestDuration).timer()
    assert(timer != null, "no request duration was recorded")
    assert(timer.count() == 1L)
    assert(timer.getId.getTag("outcome") == RequestOutcome.Ok)
    // The route pattern, never the raw path.
    assert(timer.getId.getTag("endpoint") == "/test/ok")
    assert(timer.totalTime(TimeUnit.NANOSECONDS) > 0.0)

    val counter = registry.find(MicrometerQueryMetrics.RequestCount).counter()
    assert(counter.count() == 1.0)
    assert(counter.getId.getTag("status_class") == "2xx")

    val bytes = registry.find(MicrometerQueryMetrics.ResponseBytes).summary()
    assert(bytes != null && bytes.totalAmount() > 0.0)
  }

  test("in-flight returns to zero after the response completes") {
    val registry = new SimpleMeterRegistry()
    withServer(registry) { client =>
      client.get("/test/ok").aggregate().get()
    }
    val gauge = registry.find(MicrometerQueryMetrics.RequestsInFlight).gauge()
    assert(gauge != null)
    assert(gauge.value() == 0.0)
  }

  test("a failing request is recorded rather than dropped") {
    // The gap this closes: without an outcome dimension, failures vanish from the latency view.
    val registry = new SimpleMeterRegistry()
    withServer(registry) { client =>
      val response = client.get("/test/boom").aggregate().get()
      assert(response.status().code() == 500)
    }

    val timer = registry.find(MicrometerQueryMetrics.RequestDuration).timer()
    assert(timer.count() == 1L)
    assert(timer.getId.getTag("outcome") == RequestOutcome.ServerError)
    assert(
      registry.find(MicrometerQueryMetrics.RequestCount).counter().getId.getTag("status_class") ==
        "5xx")
  }

  test("a handler can refine the query class and tenant of the request it is serving") {
    val registry = new SimpleMeterRegistry()
    withServer(registry, tenantLabel = true) { client =>
      client.get("/test/labelled").aggregate().get()
    }

    val timer = registry.find(MicrometerQueryMetrics.RequestDuration).timer()
    assert(timer.getId.getTag("query_class") == QueryClass.Incremental)
    assert(timer.getId.getTag("tenant") == "tenant-xyz")
  }

  test("an unlabelled request falls back to the route-derived class") {
    val registry = new SimpleMeterRegistry()
    withServer(registry) { client =>
      client.get("/test/ok").aggregate().get()
    }
    // /test/ok matches no sharing route, so the fallback classifies it as other.
    assert(
      registry.find(MicrometerQueryMetrics.RequestDuration).timer().getId.getTag("query_class") ==
        QueryClass.Other)
  }

  test("every request is measured exactly once") {
    val registry = new SimpleMeterRegistry()
    withServer(registry) { client =>
      (1 to 5).foreach(_ => client.get("/test/ok").aggregate().get())
    }
    val timers = registry.find(MicrometerQueryMetrics.RequestDuration).timers().asScala
    assert(timers.map(_.count()).sum == 5L)
  }

  test("an authorization rejection is still counted") {
    // Regression test for decorator ordering. Armeria runs the most recently registered
    // decorator outermost, so the metrics decorator has to be registered after the
    // authorization decorator -- the other way round, every 401 is dropped silently and the
    // error rate looks perfect while clients are being turned away.
    val registry = new SimpleMeterRegistry()
    val config = new MetricsConfig()
    config.setEnabled(true)
    config.setProjectId("test-project")
    val metrics = new MicrometerQueryMetrics(registry, config)

    val builder = Server.builder()
      .http(0)
      .annotatedService("/test", new TestService(): Any)
    // Same registration order as DeltaSharingService.start: authorization first, metrics last.
    builder.decorator(
      AuthService.builder.addOAuth2((_: ServiceRequestContext, token: OAuth2Token) => {
        CompletableFuture.completedFuture(token.accessToken == "good-token")
      }).newDecorator)
    builder.decorator(new java.util.function.Function[HttpService, HttpService] {
      override def apply(delegate: HttpService): HttpService =
        new RequestMetricsService(delegate, metrics, 300L)
    })

    val server = builder.build()
    server.start().get()
    try {
      val client = WebClient.of(s"http://127.0.0.1:${server.activeLocalPort()}")
      assert(client.get("/test/ok").aggregate().get().status().code() == 401)
    } finally {
      server.stop().get()
    }

    val timer = registry.find(MicrometerQueryMetrics.RequestDuration).timer()
    assert(timer != null, "an unauthorized request was not recorded at all")
    assert(timer.count() == 1L)
    assert(timer.getId.getTag("outcome") == RequestOutcome.ClientError)
    assert(
      registry.find(MicrometerQueryMetrics.RequestCount).counter().getId.getTag("status_class") ==
        "4xx")
  }
}
