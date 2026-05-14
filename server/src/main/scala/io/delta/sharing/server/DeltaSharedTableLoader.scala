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

package io.delta.sharing.server

import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import io.delta.standalone.internal.DeltaSharedTable
import org.slf4j.LoggerFactory

import io.delta.sharing.kernel.internal.DeltaSharedTableKernel
import io.delta.sharing.server.config.{ServerConfig, TableConfig}


/**
 * A class to load Delta tables from `TableConfig`. It also caches the loaded tables internally
 * to speed up the loading.
 */
class DeltaSharedTableLoader(serverConfig: ServerConfig) {
  private val logger = LoggerFactory.getLogger(classOf[DeltaSharedTableLoader])

  private val deltaSharedTableCache = {
    CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .maximumSize(serverConfig.deltaTableCacheSize)
      .build[String, DeltaSharedTable]()
  }

  private def measureExecTime[T](code: => T): (T, Long) = {
    val start = System.nanoTime()
    val result = code
    val elapsedNs = System.nanoTime() - start
    (result, elapsedNs)
  }

  /**
   * @return table handle and time spent in `deltaLog.update()`
   */
  def loadTableWithUpdateCost(
      tableConfig: TableConfig,
      useKernel: Boolean = false): (DeltaSharedTableProtocol, Long) = {
    if (useKernel) {
      return (
        new DeltaSharedTableKernel(
          tableConfig,
          serverConfig.preSignedUrlTimeoutSeconds,
          serverConfig.evaluatePredicateHints,
          serverConfig.evaluateJsonPredicateHints,
          serverConfig.evaluateJsonPredicateHintsV2,
          serverConfig.queryTablePageSizeLimit,
          serverConfig.queryTablePageTokenTtlMs,
          serverConfig.refreshTokenTtlMs
        ),
        0L)
    }
    try {
      val deltaSharedTable =
        deltaSharedTableCache.get(
          tableConfig.location,
          () => {
            new DeltaSharedTable(
              tableConfig,
              serverConfig.preSignedUrlTimeoutSeconds,
              serverConfig.evaluatePredicateHints,
              serverConfig.evaluateJsonPredicateHints,
              serverConfig.evaluateJsonPredicateHintsV2,
              serverConfig.queryTablePageSizeLimit,
              serverConfig.queryTablePageTokenTtlMs,
              serverConfig.refreshTokenTtlMs
            )
          }
        )
      var updateNs = 0L
      if (!serverConfig.stalenessAcceptable) {
        val (_, elapsed) = measureExecTime(deltaSharedTable.update())
        updateNs = elapsed
      }
      (deltaSharedTable, updateNs)
    } catch {
      case CausedBy(e: DeltaSharingUnsupportedOperationException) => throw e
      case e: Throwable => throw e
    }
  }

  def loadTable(tableConfig: TableConfig, useKernel: Boolean = false): DeltaSharedTableProtocol = {
    val (table, updateNs) = loadTableWithUpdateCost(tableConfig, useKernel)
    if (updateNs > 0 && serverConfig.perfLoggingEnabled) {
      logger.info(s"deltaLog.update took ${updateNs / 1000000}ms for ${tableConfig.location}")
    }
    table
  }
}
