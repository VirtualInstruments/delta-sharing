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

import scala.util.control.NonFatal

import com.linecorp.armeria.common.{HttpRequest, HttpResponse}
import com.linecorp.armeria.common.logging.RequestLog
import com.linecorp.armeria.server.{HttpService, RequestTimeoutException, ServiceRequestContext, SimpleDecoratingHttpService}
import io.netty.util.AttributeKey
import org.slf4j.LoggerFactory

/**
 * Per-request context that handlers hand to the metrics decorator.
 *
 * The decorator sees the route and the response but not the request body, so it cannot tell an
 * `incremental` read from a `snapshot` one. Handlers refine the classification through these
 * attributes once they have parsed the request; the decorator falls back to the route-derived
 * class when a handler rejected the request before getting that far.
 */
object RequestMetrics {
  private val logger = LoggerFactory.getLogger(RequestMetrics.getClass)

  private[telemetry] val QueryClassAttr: AttributeKey[String] =
    AttributeKey.valueOf("io.delta.sharing.server.metrics.queryClass")
  private[telemetry] val TenantAttr: AttributeKey[String] =
    AttributeKey.valueOf("io.delta.sharing.server.metrics.tenant")

  /**
   * Record the query class of the request being served on this thread.
   *
   * A no-op outside a request context (unit tests calling handlers directly), so handlers can call
   * it unconditionally.
   */
  def setQueryClass(queryClass: String): Unit = {
    withContext(_.setAttr(QueryClassAttr, queryClass))
  }

  /** Record the tenant of the request being served on this thread. */
  def setTenant(tenant: String): Unit = {
    if (tenant != null && tenant.nonEmpty) {
      withContext(_.setAttr(TenantAttr, tenant))
    }
  }

  private def withContext(f: ServiceRequestContext => Unit): Unit = {
    try {
      Option(ServiceRequestContext.currentOrNull()).foreach(f)
    } catch {
      case NonFatal(e) =>
        // Metrics labelling must never interfere with serving a query.
        logger.debug("Could not attach metrics attributes to the request context", e)
    }
  }
}

/**
 * Armeria decorator recording request-level metrics for every endpoint.
 *
 * This is what closes the "`version`/`metadata` have no timing at all" gap: those endpoints have
 * no stage instrumentation of their own, but a streaming client polls `version` once per
 * micro-batch, so its latency matters as much as the query it precedes.
 *
 * @param requestTimeoutSeconds used to flag requests that came within 75% of the timeout, the same
 *                              threshold as the existing near-timeout warning logs
 */
class RequestMetricsService(
    delegate: HttpService,
    metrics: QueryMetrics,
    requestTimeoutSeconds: Long)
  extends SimpleDecoratingHttpService(delegate) {

  import RequestMetricsService._

  override def serve(ctx: ServiceRequestContext, req: HttpRequest): HttpResponse = {
    val endpoint = endpointOf(ctx)
    metrics.requestStarted(endpoint)
    ctx.log().whenComplete().thenAccept { (log: RequestLog) =>
      try {
        record(ctx, endpoint, log)
      } catch {
        case NonFatal(e) =>
          logger.warn("Failed to record request metrics for {}", endpoint.asInstanceOf[Any], e)
      }
    }
    delegate.serve(ctx, req)
  }

  private def record(ctx: ServiceRequestContext, endpoint: String, log: RequestLog): Unit = {
    val durationNs = log.totalDurationNanos()
    val queryClass = Option(ctx.attr(RequestMetrics.QueryClassAttr))
      .getOrElse(QueryClass.forRoute(endpoint))
    val tenant = Option(ctx.attr(RequestMetrics.TenantAttr))
    val status = Option(log.responseHeaders()).map(_.status().code()).getOrElse(0)

    metrics.requestFinished(
      endpoint = endpoint,
      queryClass = queryClass,
      outcome = outcomeOf(status, log.responseCause()),
      statusClass = statusClassOf(status),
      tenant = tenant,
      durationNs = durationNs,
      ttfbNs = timeToFirstByteNs(log),
      responseBytes = log.responseLength(),
      nearTimeout = isNearTimeout(durationNs, requestTimeoutSeconds))
  }
}

object RequestMetricsService {
  private val logger = LoggerFactory.getLogger(classOf[RequestMetricsService])

  /** The fraction of the request timeout above which a request is worth counting separately. */
  private val NearTimeoutFraction = 0.75

  /** Matched route pattern (never the raw path, which would be unbounded cardinality). */
  private[telemetry] def endpointOf(ctx: ServiceRequestContext): String = {
    Option(ctx.config().route().patternString()).getOrElse("unknown")
  }

  private[telemetry] def statusClassOf(status: Int): String = {
    if (status >= 200 && status < 300) {
      "2xx"
    } else if (status >= 400 && status < 500) {
      "4xx"
    } else if (status >= 500) {
      "5xx"
    } else {
      "unknown"
    }
  }

  /**
   * Failed requests must not vanish from the latency view, so every request gets an outcome.
   * The cause takes precedence over the status: a timeout or a client disconnect is a different
   * failure from a 500 the handler chose to return.
   */
  private[telemetry] def outcomeOf(status: Int, cause: Throwable): String = {
    if (cause != null) {
      cause match {
        case _: RequestTimeoutException => RequestOutcome.Timeout
        case _ if isClientDisconnect(cause) => RequestOutcome.ClientDisconnect
        case _ => RequestOutcome.ServerError
      }
    } else if (status >= 200 && status < 400) {
      RequestOutcome.Ok
    } else if (status >= 400 && status < 500) {
      RequestOutcome.ClientError
    } else if (status >= 500) {
      RequestOutcome.ServerError
    } else {
      RequestOutcome.Unknown
    }
  }

  private def isClientDisconnect(cause: Throwable): Boolean = {
    val name = cause.getClass.getSimpleName
    name == "ClosedSessionException" || name == "ClosedStreamException" ||
      name == "AbortedStreamException"
  }

  private[telemetry] def isNearTimeout(durationNs: Long, requestTimeoutSeconds: Long): Boolean = {
    requestTimeoutSeconds > 0 &&
      durationNs > (requestTimeoutSeconds * 1000000000L * NearTimeoutFraction)
  }

  /**
   * Time from the request arriving to the first response byte leaving.
   *
   * Worth tracking separately even though today's query paths build the whole response before
   * streaming any of it: that makes TTFB ~ total, and the day that changes the two diverge.
   */
  private[telemetry] def timeToFirstByteNs(log: RequestLog): Option[Long] = {
    Option(log.responseFirstBytesTransferredTimeNanos())
      .map(first => first.longValue() - log.requestStartTimeNanos())
      .filter(_ >= 0)
  }
}
