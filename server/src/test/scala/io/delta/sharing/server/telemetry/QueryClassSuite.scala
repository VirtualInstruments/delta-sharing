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

import org.scalatest.FunSuite

class QueryClassSuite extends FunSuite {

  private def classify(
      version: Boolean = false,
      timestamp: Boolean = false,
      startingVersion: Boolean = false,
      predicateHints: Boolean = false,
      jsonPredicateHints: Boolean = false,
      maxFiles: Boolean = false,
      pageToken: Boolean = false): String = {
    QueryClass.forTableQuery(version, timestamp, startingVersion, predicateHints,
      jsonPredicateHints, maxFiles, pageToken)
  }

  test("a bare query is a snapshot query") {
    assert(classify() == QueryClass.Snapshot)
  }

  test("startingVersion outranks every other parameter") {
    // A streaming read stays an incremental read however else it is narrowed, because it is the
    // commit replay that determines its cost.
    assert(classify(startingVersion = true) == QueryClass.Incremental)
    assert(classify(startingVersion = true, maxFiles = true) == QueryClass.Incremental)
    assert(
      classify(startingVersion = true, predicateHints = true, pageToken = true) ==
        QueryClass.Incremental)
  }

  test("a version or timestamp pin outranks predicate filtering") {
    assert(classify(version = true) == QueryClass.SnapshotAsOf)
    assert(classify(timestamp = true) == QueryClass.SnapshotAsOf)
    assert(classify(version = true, predicateHints = true) == QueryClass.SnapshotAsOf)
  }

  test("predicate hints, maxFiles or a page token make a filtered snapshot") {
    assert(classify(predicateHints = true) == QueryClass.SnapshotFiltered)
    assert(classify(maxFiles = true) == QueryClass.SnapshotFiltered)
    assert(classify(pageToken = true) == QueryClass.SnapshotFiltered)
  }

  test("a json predicate alone makes a filtered snapshot") {
    // The kernel engine serves these and applies the predicate itself, so classifying them as a
    // bare snapshot would mix filtered work into the snapshot latency distribution.
    assert(classify(jsonPredicateHints = true) == QueryClass.SnapshotFiltered)
  }

  test("classification describes the request, not the engine that serves it") {
    assert(classify() == QueryClass.Snapshot)
    assert(classify(version = true) == QueryClass.SnapshotAsOf)
    // Each of the four parameters that route to Delta Standalone moves the class off Snapshot.
    assert(classify(predicateHints = true) != QueryClass.Snapshot)
    assert(classify(maxFiles = true) != QueryClass.Snapshot)
    assert(classify(startingVersion = true) != QueryClass.Snapshot)
    assert(classify(pageToken = true) != QueryClass.Snapshot)
    // jsonPredicateHints deliberately does NOT line up with the engine split: the kernel engine
    // still serves it, so snapshot_filtered legitimately appears with engine=kernel.
    assert(classify(jsonPredicateHints = true) == QueryClass.SnapshotFiltered)
  }

  test("routes map to the class fixed by the endpoint") {
    val prefix = "/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}"
    assert(QueryClass.forRoute(s"$prefix/version") == QueryClass.Version)
    assert(
      QueryClass.forRoute("/delta-sharing/shares/{share}/schemas/{schema}/tables/{table}") ==
        QueryClass.Version)
    assert(QueryClass.forRoute(s"$prefix/metadata") == QueryClass.Metadata)
    assert(QueryClass.forRoute(s"$prefix/changes") == QueryClass.Cdf)
    assert(QueryClass.forRoute(s"$prefix/query") == QueryClass.Snapshot)
    assert(QueryClass.forRoute("/delta-sharing/shares") == QueryClass.Catalog)
    assert(QueryClass.forRoute("/delta-sharing/shares/{share}/all-tables") == QueryClass.Catalog)
  }

  test("an unknown or absent route does not throw") {
    assert(QueryClass.forRoute(null) == QueryClass.Other)
    assert(QueryClass.forRoute("/healthz") == QueryClass.Other)
  }

  test("page label distinguishes continuations") {
    assert(QueryClass.pageLabel(hasPageToken = false) == QueryClass.PageFirst)
    assert(QueryClass.pageLabel(hasPageToken = true) == QueryClass.PageContinuation)
  }
}
