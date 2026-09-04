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

/**
 * Classification of incoming requests by query shape.
 *
 * Latency has to be sliced by query class because the code paths behind them barely overlap: a
 * `version` poll resolves a snapshot and returns a header, while an `incremental` read replays
 * every commit since the client's last checkpoint. Aggregating them into one latency figure hides
 * both.
 *
 * See memory-bank/08-query-performance-metrics.md section 1 for the taxonomy and the routing table.
 */
object QueryClass {
  /** `GET /version` and `HEAD /tables/{table}` -- the structured-streaming poll. */
  val Version = "version"
  /** `GET /metadata`. */
  val Metadata = "metadata"
  /** `POST /query` with no version, predicate, paging or streaming parameters. */
  val Snapshot = "snapshot"
  /**
   * `POST /query` narrowed by predicate hints, `maxFiles` or a page token.
   *
   * Note this does not line up with the engine split: `jsonPredicateHints` alone still routes to
   * the Kernel engine, which applies the predicate itself, so this class appears with both
   * `engine=standalone` and `engine=kernel`. Classifying by what the client asked for rather than
   * by which engine served it is deliberate -- a filtered query is a filtered query.
   */
  val SnapshotFiltered = "snapshot_filtered"
  /** `POST /query` pinned to a `version` or `timestamp`. */
  val SnapshotAsOf = "snapshot_asof"
  /** `POST /query` with `startingVersion` -- the streaming incremental read. */
  val Incremental = "incremental"
  /** `GET /changes` -- change data feed. */
  val Cdf = "cdf"
  /** Share/schema/table listing endpoints; served from config with no object-store I/O. */
  val Catalog = "catalog"
  /** Anything not recognised, including the async-query acknowledgement path. */
  val Other = "other"

  /** Engine that served a request, for the `engine` metric label. */
  val EngineStandalone = "standalone"
  val EngineKernel = "kernel"

  /** Value of the `paged` label. */
  val PageFirst = "first"
  val PageContinuation = "continuation"

  /**
   * Classify a `POST /query` request from its parameters.
   *
   * The order of the checks mirrors the precedence in `DeltaSharingService.listFiles`: a request
   * carrying `startingVersion` is an incremental read regardless of what else it sets, and a
   * version/timestamp pin outranks predicate filtering.
   */
  def forTableQuery(
      hasVersion: Boolean,
      hasTimestamp: Boolean,
      hasStartingVersion: Boolean,
      hasPredicateHints: Boolean,
      hasJsonPredicateHints: Boolean,
      hasMaxFiles: Boolean,
      hasPageToken: Boolean): String = {
    if (hasStartingVersion) {
      Incremental
    } else if (hasVersion || hasTimestamp) {
      SnapshotAsOf
    } else if (hasPredicateHints || hasJsonPredicateHints || hasMaxFiles || hasPageToken) {
      SnapshotFiltered
    } else {
      Snapshot
    }
  }

  /**
   * Best-effort classification from the matched route pattern.
   *
   * Used by the request decorator for endpoints whose class is fixed by the route, and as the
   * fallback when a handler failed before it could refine the class (a validation rejection, for
   * example). Body-dependent classes are refined later by [[RequestMetrics.setQueryClass]].
   */
  def forRoute(pattern: String): String = {
    if (pattern == null) {
      Other
    } else if (pattern.endsWith("/version") || pattern.endsWith("/tables/{table}")) {
      Version
    } else if (pattern.endsWith("/metadata")) {
      Metadata
    } else if (pattern.endsWith("/changes")) {
      Cdf
    } else if (pattern.endsWith("/query")) {
      // Refined by the handler once the request body has been parsed.
      Snapshot
    } else if (pattern.contains("/queries/")) {
      // The async query-status route. It reads table state rather than listing config, so it
      // must not fall through to the catalog bucket below -- catalog latency is otherwise all
      // sub-second config lookups, and mixing real table work into it makes that unreadable.
      Other
    } else if (pattern.contains("/shares")) {
      Catalog
    } else {
      Other
    }
  }

  /** Whether a request continues an earlier paged response. */
  def pageLabel(hasPageToken: Boolean): String =
    if (hasPageToken) PageContinuation else PageFirst
}
