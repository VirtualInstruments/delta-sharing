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

import com.linecorp.armeria.server.RequestTimeoutException
import org.scalatest.FunSuite

class RequestMetricsServiceSuite extends FunSuite {

  test("status classes are bucketed") {
    assert(RequestMetricsService.statusClassOf(200) == "2xx")
    assert(RequestMetricsService.statusClassOf(204) == "2xx")
    assert(RequestMetricsService.statusClassOf(400) == "4xx")
    assert(RequestMetricsService.statusClassOf(404) == "4xx")
    assert(RequestMetricsService.statusClassOf(500) == "5xx")
    assert(RequestMetricsService.statusClassOf(0) == "unknown")
  }

  test("outcome is derived from the status when the request did not fail") {
    assert(RequestMetricsService.outcomeOf(200, null) == RequestOutcome.Ok)
    assert(RequestMetricsService.outcomeOf(304, null) == RequestOutcome.Ok)
    assert(RequestMetricsService.outcomeOf(400, null) == RequestOutcome.ClientError)
    assert(RequestMetricsService.outcomeOf(500, null) == RequestOutcome.ServerError)
    assert(RequestMetricsService.outcomeOf(0, null) == RequestOutcome.Unknown)
  }

  test("a timeout is reported as a timeout, not as a server error") {
    // Timeouts are the failure mode customers actually hit, so they must not be folded into 5xx.
    val outcome = RequestMetricsService.outcomeOf(503, RequestTimeoutException.get())
    assert(outcome == RequestOutcome.Timeout)
  }

  test("the cause takes precedence over the status") {
    assert(RequestMetricsService.outcomeOf(200, new RuntimeException("boom")) ==
      RequestOutcome.ServerError)
  }

  test("near timeout fires at 75 percent of the request timeout") {
    val timeoutSeconds = 300L
    val threshold = (300L * 1000000000L * 0.75).toLong
    assert(!RequestMetricsService.isNearTimeout(threshold - 1000L, timeoutSeconds))
    assert(RequestMetricsService.isNearTimeout(threshold + 1000L, timeoutSeconds))
  }

  test("near timeout is never reported when timeouts are disabled") {
    assert(!RequestMetricsService.isNearTimeout(Long.MaxValue, 0L))
  }
}
