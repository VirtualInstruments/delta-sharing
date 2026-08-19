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

// Putting these classes in this package to access Delta Standalone internal APIs
package io.delta.standalone.internal

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.google.common.hash.Hashing
import io.delta.standalone.DeltaLog
import io.delta.standalone.internal.actions.{AddCDCFile, AddFile, Metadata, Protocol, RemoveFile}
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.ConversionUtils
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.azure.NativeAzureFileSystem
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import io.delta.sharing.server.{model, CdfQueryTimings, CdfTimings, DeltaSharedTableProtocol, DeltaSharingIllegalArgumentException, DeltaSharingUnsupportedOperationException, ErrorStrings, QueryResult, TableQueryTimings, TableTimings}
import io.delta.sharing.server.common.{AbfsFileSigner, GCSFileSigner, JsonUtils, PreSignedUrl, S3FileSigner, WasbFileSigner}
import io.delta.sharing.server.config.TableConfig
import io.delta.sharing.server.protocol.{QueryTablePageToken, RefreshToken}

/**
 * A util class stores all query parameters. Used to compute the checksum in the page token for
 * query validation.
 */
private case class QueryParamChecksum(
    version: Option[Long],
    timestamp: Option[String],
    startingVersion: Option[Long],
    startingTimestamp: Option[String],
    endingVersion: Option[Long],
    endingTimestamp: Option[String],
    predicateHints: Seq[String],
    jsonPredicateHints: Option[String],
    limitHint: Option[Long],
    includeHistoricalMetadata: Option[Boolean])



/**
 * A table class that wraps `DeltaLog` to provide the methods used by the server.
 */
class DeltaSharedTable(
    tableConfig: TableConfig,
    preSignedUrlTimeoutSeconds: Long,
    evaluatePredicateHints: Boolean,
    evaluateJsonPredicateHints: Boolean,
    evaluateJsonPredicateHintsV2: Boolean,
    queryTablePageSizeLimit: Int,
    queryTablePageTokenTtlMs: Int,
    refreshTokenTtlMs: Int) extends DeltaSharedTableProtocol {

  private val logger = LoggerFactory.getLogger(classOf[DeltaSharedTable])

  private val conf = withClassLoader {
    new Configuration()
  }

  private val deltaLog = withClassLoader {
    val tablePath = new Path(tableConfig.getLocation)
    try {
      DeltaLog.forTable(conf, tablePath).asInstanceOf[DeltaLogImpl]
    } catch {
      // convert InvalidProtocolVersionException to client error(400)
      case e: DeltaErrors.InvalidProtocolVersionException =>
        throw new DeltaSharingUnsupportedOperationException(e.getMessage)
      case e: Throwable => throw e
    }
  }

  private val fileSigner = withClassLoader {
    val tablePath = new Path(tableConfig.getLocation)
    val fs = tablePath.getFileSystem(conf)
    fs match {
      case _: S3AFileSystem =>
        new S3FileSigner(deltaLog.dataPath.toUri, conf, preSignedUrlTimeoutSeconds)
      case wasb: NativeAzureFileSystem =>
        WasbFileSigner(wasb, deltaLog.dataPath.toUri, conf, preSignedUrlTimeoutSeconds)
      case abfs: AzureBlobFileSystem =>
        AbfsFileSigner(abfs, deltaLog.dataPath.toUri, preSignedUrlTimeoutSeconds)
      case gc: GoogleHadoopFileSystem =>
        new GCSFileSigner(deltaLog.dataPath.toUri, conf, preSignedUrlTimeoutSeconds)
      case _ =>
        throw new IllegalStateException(s"File system ${fs.getClass} is not supported")
    }
  }

  /**
   * Run `func` under the classloader of `DeltaSharedTable`. We cannot use the classloader set by
   * Armeria as Hadoop needs to search the classpath to find its classes.
   */
  private def withClassLoader[T](func: => T): T = {
    val classLoader = Thread.currentThread().getContextClassLoader
    if (classLoader == null) {
      Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
      try func finally {
        Thread.currentThread().setContextClassLoader(null)
      }
    } else {
      func
    }
  }

  /**
   * Sign multiple file paths in parallel and return the signed URLs in the same order.
   */
  private def parallelSign(paths: Seq[Path]): Seq[PreSignedUrl] = {
    if (paths.isEmpty) {
      return Seq.empty
    }

    if (paths.size == 1) {
      // Skip parallel overhead for single path
      return Seq(fileSigner.sign(paths.head))
    }

    implicit val ec: ExecutionContext = DeltaSharedTable.signingExecutionContext

    val signFutures = paths.map { path =>
      Future {
        withClassLoader {
          fileSigner.sign(path)
        }
      }
    }

    val allFutures = Future.sequence(signFutures)    
    // scalastyle:off awaitresult
    Await.result(allFutures, Duration.Inf)
    // scalastyle:on awaitresult
  }

  /** Check if the table version in deltalog is valid */
  private def validateDeltaTable(snapshot: SnapshotImpl): Unit = {
    if (snapshot.version < 0) {
      throw new IllegalStateException(s"The table ${tableConfig.getName} " +
        s"doesn't exist on the file system or is not a Delta table")
    }
  }

  /** Get table version at or after startingTimestamp if it's provided, otherwise return
   *  the latest table version.
   */
  override def getTableVersion(startingTimestamp: Option[String]): Long = withClassLoader {
    if (startingTimestamp.isEmpty) {
      tableVersion
    } else {
      val ts = DeltaSharingHistoryManager.getTimestamp("startingTimestamp", startingTimestamp.get)
      // get a version at or after the provided timestamp, if the timestamp is early than version 0,
      // return 0.
      try {
        deltaLog.getVersionAtOrAfterTimestamp(ts.getTime())
      } catch {
        // Convert to DeltaSharingIllegalArgumentException to return 4xx instead of 5xx error code
        // Only convert known exceptions around timestamp too late or too early
        case e: IllegalArgumentException =>
          throw new DeltaSharingIllegalArgumentException(e.getMessage)
      }
    }
  }

  /** Return the current table version */
  def tableVersion: Long = withClassLoader {
    val snapshot = deltaLog.snapshot
    validateDeltaTable(snapshot)
    snapshot.version
  }

  // Construct the protocol class to be returned in the response based on the responseFormat.
  private def getResponseProtocol(p: Protocol, responseFormat: String): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      DeltaResponseProtocol(deltaProtocol = p).wrap
    } else {
      model.Protocol(p.minReaderVersion).wrap
    }
  }

  // Construct the metadata class to be returned in the response based on the responseFormat.
  private def getResponseMetadata(
      m: Metadata,
      startingVersion: Option[Long],
      responseFormat: String
  ): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      DeltaResponseMetadata(
        version = if (startingVersion.isDefined) {
          startingVersion.get
        } else {
          null
        },
        deltaMetadata = DeltaMetadataCopy(m)
      ).wrap
    } else {
      model.Metadata(
        id = m.id,
        name = m.name,
        description = m.description,
        format = model.Format(),
        schemaString = cleanUpTableSchema(m.schemaString),
        configuration = getMetadataConfiguration(m.configuration),
        partitionColumns = m.partitionColumns,
        version = if (startingVersion.isDefined) {
          startingVersion.get
        } else {
          null
        }
      ).wrap
    }
  }

  // Construct the returning class for addFile based on requested responseFormat.
  private def getResponseAddFile(
      addFile: AddFile,
      signedUrl: PreSignedUrl,
      version: java.lang.Long,
      timestamp: java.lang.Long,
      responseFormat: String,
      returnAddFileForCDF: Boolean = false): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      DeltaResponseFileAction(
        id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        version = version,
        timestamp = timestamp,
        deltaSingleAction = addFile.copy(path = signedUrl.url).wrap
      ).wrap
    } else if (returnAddFileForCDF) {
      model.AddFileForCDF(
        url = signedUrl.url,
        id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = addFile.partitionValues,
        size = addFile.size,
        stats = addFile.stats,
        version = version,
        timestamp = timestamp
      ).wrap
    } else {
      model.AddFile(
        url = signedUrl.url,
        id = Hashing.md5().hashString(addFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = addFile.partitionValues,
        size = addFile.size,
        stats = addFile.stats,
        version = version,
        timestamp = timestamp
      ).wrap
    }
  }

  // Construct and return the RemoveFile class to be returned in the response based on the
  // responseFormat.
  private def getResponseRemoveFile(
    removeFile: RemoveFile,
    signedUrl: PreSignedUrl,
    version: java.lang.Long,
    timestamp: java.lang.Long,
    responseFormat: String): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      DeltaResponseFileAction(
        id = Hashing.md5().hashString(removeFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        version = version,
        timestamp = timestamp,
        deltaSingleAction = removeFile.copy(path = signedUrl.url).wrap
      ).wrap
    } else {
      model.RemoveFile(
        url = signedUrl.url,
        id = Hashing.md5().hashString(removeFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = removeFile.partitionValues,
        size = removeFile.size.get,
        version = version,
        timestamp = timestamp
      ).wrap
    }
  }

  // Construct and return the AddCDCFile class to be returned in the response based on the
  // responseFormat.
  private def getResponseAddCDCFile(
    addCDCFile: AddCDCFile,
    signedUrl: PreSignedUrl,
    version: java.lang.Long,
    timestamp: java.lang.Long,
    responseFormat: String
  ): Object = {
    if (responseFormat == DeltaSharedTable.RESPONSE_FORMAT_DELTA) {
      DeltaResponseFileAction(
        id = Hashing.md5().hashString(addCDCFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        version = version,
        timestamp = timestamp,
        deltaSingleAction = addCDCFile.copy(path = signedUrl.url).wrap
      ).wrap
    } else {
      model.AddCDCFile(
        url = signedUrl.url,
        id = Hashing.md5().hashString(addCDCFile.path, UTF_8).toString,
        expirationTimestamp = signedUrl.expirationTimestamp,
        partitionValues = addCDCFile.partitionValues,
        size = addCDCFile.size,
        version = version,
        timestamp = timestamp
      ).wrap
    }
  }

  // Construct and return the end action of the streaming response.
  private def getEndStreamAction(
      nextPageTokenStr: String,
      minUrlExpirationTimestamp: Long,
      refreshTokenStr: String = null): model.SingleAction = {
    model.EndStreamAction(
      refreshTokenStr,
      nextPageTokenStr,
      if (minUrlExpirationTimestamp == Long.MaxValue) null else minUrlExpirationTimestamp
    ).wrap
  }

  // scalastyle:off argcount
  def query(
      includeFiles: Boolean,
      predicateHints: Seq[String],
      jsonPredicateHints: Option[String],
      limitHint: Option[Long],
      version: Option[Long],
      timestamp: Option[String],
      startingVersion: Option[Long],
      endingVersion: Option[Long],
      maxFiles: Option[Int],
      pageToken: Option[String],
      includeRefreshToken: Boolean,
      refreshToken: Option[String],
      responseFormatSet: Set[String],
      clientReaderFeaturesSet: Set[String],
      includeEndStreamAction: Boolean,
      deltaLogUpdateNs: Long = 0L,
      requestTimeoutSecondsForLogging: Option[Long] = None): QueryResult = withClassLoader {
    // scalastyle:on argcount
    // TODO Support `limitHint`
    if (Seq(version, timestamp, startingVersion).filter(_.isDefined).size >= 2) {
      throw new DeltaSharingIllegalArgumentException(
        ErrorStrings.multipleParametersSetErrorMsg(Seq("version", "timestamp", "startingVersion"))
      )
    }
    // Validate pageToken if it's specified
    lazy val queryParamChecksum = computeChecksum(
      QueryParamChecksum(
        version = version,
        timestamp = timestamp,
        startingVersion = startingVersion,
        startingTimestamp = None,
        endingVersion = endingVersion,
        endingTimestamp = None,
        predicateHints = predicateHints,
        jsonPredicateHints = jsonPredicateHints,
        limitHint = limitHint,
        includeHistoricalMetadata = None
      )
    )
    val pageTokenOpt = pageToken.map(decodeAndValidatePageToken(_, queryParamChecksum))
    // Validate refreshToken if it's specified
    val refreshTokenOpt = refreshToken.map(decodeAndValidateRefreshToken)
    val tSnapshotResolve = System.nanoTime()
    // The version of the snapshot should follow the below precedence:
    // 1. Use version specified in the pageToken, which is equal to the version we use in the
    //    first page request. This is to make sure that responses are consistent across pages.
    // 2. Use version/timestamp/startingVersion specified by the user.
    // 3. Use version specified in the refreshToken, which is equal to latest table version upon
    //    initial request. In this case, it must be a latest snapshot query and version/timestamp/
    //    startingVersion must not be specified.
    val specifiedVersion = pageTokenOpt.flatMap(_.version)
      .orElse(version)
      .orElse(startingVersion)
      .orElse(refreshTokenOpt.map(_.getVersion))
    val snapshot =
      if (specifiedVersion.isDefined) {
        try {
          deltaLog.getSnapshotForVersionAsOf(specifiedVersion.get)
        } catch {
          case e: io.delta.standalone.exceptions.DeltaStandaloneException =>
            throw new DeltaSharingIllegalArgumentException(e.getMessage)
        }
      } else if (timestamp.isDefined) {
        val ts = DeltaSharingHistoryManager.getTimestamp("timestamp", timestamp.get)
        try {
          deltaLog.getSnapshotForTimestampAsOf(ts.getTime())
        } catch {
          // Convert to DeltaSharingIllegalArgumentException to return 4xx instead of 5xx error code
          // Only convert known exceptions around timestamp too late or too early
          case e: IllegalArgumentException =>
            throw new DeltaSharingIllegalArgumentException(e.getMessage)
        }
      } else {
        deltaLog.snapshot
      }
    val snapshotResolveNs = System.nanoTime() - tSnapshotResolve
    // TODO Open the `state` field in Delta Standalone library.
    val stateMethod = snapshot.getClass.getMethod("state")
    val state = stateMethod.invoke(snapshot).asInstanceOf[SnapshotImpl.State]

    val isVersionQuery = !Seq(version, timestamp).filter(_.isDefined).isEmpty
    // If the client accept parquet format and it's a basic table, return as parquet format.
    val responseFormat = if (snapshot.protocolScala.minReaderVersion == 1 &&
      responseFormatSet.contains(DeltaSharedTable.RESPONSE_FORMAT_PARQUET)) {
      DeltaSharedTable.RESPONSE_FORMAT_PARQUET
    } else {
      DeltaSharedTable.RESPONSE_FORMAT_DELTA
    }
    val (tailActions, tableTimings) = {
      if (startingVersion.isDefined) {
        // Only read changes up to snapshot.version, and ignore changes that are committed during
        // queryDataChangeSinceStartVersion.
        val (changeActions, tsNs, replayNs, signNs, verCount, qStart, qEnd) =
          queryDataChangeSinceStartVersion(
            startingVersion.get,
            endingVersion,
            maxFiles,
            pageTokenOpt,
            queryParamChecksum,
            responseFormat,
            includeEndStreamAction
          )
        (
          changeActions,
          TableQueryTimings(
            deltaLogUpdateNs,
            snapshotResolveNs,
            tsNs + replayNs,
            signNs,
            Some(verCount),
            Some(qStart),
            Some(qEnd)))
      } else if (includeFiles) {
        val tPrepare = System.nanoTime()
        val ts = if (isVersionQuery) {
          val timestampsByVersion = DeltaSharingHistoryManager.getTimestampsByVersion(
            deltaLog.store,
            deltaLog.logPath,
            snapshot.version,
            snapshot.version + 1,
            conf
          )
          Some(timestampsByVersion.get(snapshot.version).orNull.getTime)
        } else {
          None
        }
        // Enforce page size only when `maxFiles` is specified for backwards compatibility.
        val pageSizeOpt = maxFiles.map(_.min(queryTablePageSizeLimit))
        var nextPageTokenStr: String = null
        var minUrlExpirationTimestamp = Long.MaxValue

        // Skip files that are already processed in previous pages
        val selectedIndexedFiles = state.activeFiles.toSeq.zipWithIndex
          .drop(pageTokenOpt.map(_.getStartingActionIndex).getOrElse(0))

        // Select files that satisfy predicate hints
        val useJsonPredicateHints =
          (evaluateJsonPredicateHints && snapshot.metadataScala.partitionColumns.nonEmpty) ||
          evaluateJsonPredicateHintsV2
        var filteredIndexedFiles =
          if (useJsonPredicateHints) {
            JsonPredicateFilterUtils.evaluatePredicate(
              jsonPredicateHints,
              evaluateJsonPredicateHintsV2,
              selectedIndexedFiles
            )
          } else {
            selectedIndexedFiles
          }

        // Select files that satisfy partition hints
        filteredIndexedFiles =
          if (evaluatePredicateHints && snapshot.metadataScala.partitionColumns.nonEmpty) {
            PartitionFilterUtils.evaluatePredicate(
              snapshot.metadataScala.schemaString,
              snapshot.metadataScala.partitionColumns,
              predicateHints,
              filteredIndexedFiles
            )
          } else {
            filteredIndexedFiles
          }
        // If number of valid files is greater than page size, generate nextPageToken and
        // drop additional files.
        if (pageSizeOpt.exists(_ < filteredIndexedFiles.length)) {
          nextPageTokenStr = DeltaSharedTable.encodeToken(
            QueryTablePageToken(
              id = Some(tableConfig.id),
              version = Some(snapshot.version),
              checksum = Some(queryParamChecksum),
              startingActionIndex = Some(filteredIndexedFiles(pageSizeOpt.get)._2),
              expirationTimestamp = Some(System.currentTimeMillis() + queryTablePageTokenTtlMs)
            )
          )
          filteredIndexedFiles = filteredIndexedFiles.take(pageSizeOpt.get)
        }
        var fileSigningNs = 0L
        def timedSignSnapshotFile(cloudPath: Path): PreSignedUrl = {
          val a = System.nanoTime()
          val u = fileSigner.sign(cloudPath)
          fileSigningNs += System.nanoTime() - a
          u
        }
        val filteredFiles = filteredIndexedFiles.map {
          case (addFile, _) =>
            val cloudPath = absolutePath(deltaLog.dataPath, addFile.path)
            val signedUrl = timedSignSnapshotFile(cloudPath)
            minUrlExpirationTimestamp = minUrlExpirationTimestamp.min(signedUrl.expirationTimestamp)
            getResponseAddFile(
              addFile,
              signedUrl,
              if (isVersionQuery) { snapshot.version } else null,
              if (isVersionQuery) { ts.get } else null,
              responseFormat
            )
        }
        val refreshTokenStr = if (includeRefreshToken) {
          DeltaSharedTable.encodeToken(
            RefreshToken(
              id = Some(tableConfig.id),
              version = Some(snapshot.version),
              expirationTimestamp = Some(System.currentTimeMillis() + refreshTokenTtlMs)
            )
          )
        } else {
          null
        }
        // For backwards compatibility, return an `endStreamAction` object only when
        // `includeRefreshToken` is true, `maxFiles` is specified or includeEndStreamAction.
        val filesOut = filteredFiles ++ {
          if (includeRefreshToken || maxFiles.isDefined || includeEndStreamAction) {
            Seq(getEndStreamAction(nextPageTokenStr, minUrlExpirationTimestamp, refreshTokenStr))
          } else {
            Nil
          }
        }
        val prepareAndSignWallNs = System.nanoTime() - tPrepare
        val replayOrPrepareNs = prepareAndSignWallNs - fileSigningNs
        (
          filesOut,
          TableQueryTimings(
            deltaLogUpdateNs,
            snapshotResolveNs,
            replayOrPrepareNs,
            fileSigningNs,
            None,
            None,
            None))
      } else {
        (
          Nil,
          TableQueryTimings(deltaLogUpdateNs, snapshotResolveNs, 0L, 0L, None, None, None))
      }
    }

    val actions = Seq(
      getResponseProtocol(snapshot.protocolScala, responseFormat),
      getResponseMetadata(snapshot.metadataScala, startingVersion, responseFormat)
    ) ++ tailActions

    warnIfNearRequestTimeout(
      requestTimeoutSecondsForLogging,
      tableInternalWorkNs(tableTimings),
      "query")
    QueryResult(
      snapshot.version,
      actions,
      responseFormat,
      Some(TableTimings(tableTimings)))
  }

  private def queryDataChangeSinceStartVersion(
      startingVersion: Long,
      endingVersion: Option[Long],
      maxFilesOpt: Option[Int],
      pageTokenOpt: Option[QueryTablePageToken],
      queryParamChecksum: String,
      responseFormat: String,
      includeEndStreamAction: Boolean
    ): (Seq[Object], Long, Long, Long, Int, Long, Long) = {
    // For subsequent page calls, instead of using the current latestVersion, use latestVersion in
    // the pageToken (which is equal to the latestVersion when the first page call is received),
    // in case the latestVersion changes after the first page call.
    val latestVersion = pageTokenOpt.map(_.getLatestVersion).getOrElse(tableVersion)
    if (startingVersion > latestVersion) {
      throw DeltaCDFErrors.startVersionAfterLatestVersion(startingVersion, latestVersion)
    }
    if (endingVersion.isDefined && endingVersion.get > latestVersion) {
      throw DeltaCDFErrors.endVersionAfterLatestVersion(endingVersion.get, latestVersion)
    }
    // We use (start, end) from the page token instead of the original request because:
    // - Versions that are processed in previous pages can be skipped.
    // - Versions that are committed after the first page call should be ignored, especially
    //   when the endingVersion is not specified and resolved to latestVersion.
    val start = pageTokenOpt.map(_.getStartingVersion).getOrElse(startingVersion)
    val end = pageTokenOpt
      .map(_.getEndingVersion)
      .orElse(endingVersion)
      .getOrElse(latestVersion)
      .min(latestVersion)
    val tTs = System.nanoTime()
    val timestampsByVersion = DeltaSharingHistoryManager.getTimestampsByVersion(
      deltaLog.store,
      deltaLog.logPath,
      start,
      end + 1,
      conf
    )
    val timestampIndexNs = System.nanoTime() - tTs

    // Enforce page size only when `maxFiles` is specified for backwards compatibility.
    val pageSizeOpt = maxFilesOpt.map(_.min(queryTablePageSizeLimit))
    val tokenGenerator = { (v: Long, idx: Int) =>
      DeltaSharedTable.encodeToken(
        QueryTablePageToken(
          id = Some(tableConfig.id),
          startingVersion = Some(v),
          endingVersion = Some(end),
          latestVersion = Some(latestVersion),
          checksum = Some(queryParamChecksum),
          startingActionIndex = Some(idx),
          expirationTimestamp = Some(System.currentTimeMillis() + queryTablePageTokenTtlMs)
        )
      )
    }
    var minUrlExpirationTimestamp = Long.MaxValue
    var numSignedFiles = 0

    // Track actions that need signing and non-file actions, preserving order
    case class FileToSign(
      path: Path,
      action: Either[AddFile, RemoveFile],
      version: Long,
      timestamp: java.sql.Timestamp,
      idx: Int)

    sealed trait ActionItem
    case class SignedFileItem(fileToSign: FileToSign) extends ActionItem
    case class MetadataItem(m: Metadata, v: Long) extends ActionItem

    val filesToSign = ListBuffer[FileToSign]()
    val orderedItems = ListBuffer[ActionItem]()
    var earlyReturnToken: Option[String] = None
    var versionsIterated = 0
    val tScan = System.nanoTime()

    deltaLog
      .getChanges(start, true)
      .asScala
      .toSeq
      .filter(_.getVersion <= end)
      .foreach { versionLog =>
        if (earlyReturnToken.isEmpty) {
          versionsIterated += 1
          val v = versionLog.getVersion
          var indexedVersionActions =
            versionLog.getActions.asScala.map(x => ConversionUtils.convertActionJ(x)).zipWithIndex
          val ts = timestampsByVersion.get(v).orNull
          if (pageTokenOpt.exists(_.getStartingVersion == v)) {
            // Skip actions that are already processed in previous pages
            indexedVersionActions =
              indexedVersionActions.drop(pageTokenOpt.get.getStartingActionIndex)
          }
          indexedVersionActions.foreach {
            case (a: AddFile, idx) if a.dataChange && earlyReturnToken.isEmpty =>
              // Check if we've reached page size limit
              if (pageSizeOpt.contains(numSignedFiles)) {
                earlyReturnToken = Some(tokenGenerator(v, idx))
              } else {
                val fileToSign = FileToSign(
                  absolutePath(deltaLog.dataPath, a.path),
                  Left(a),
                  v,
                  ts,
                  idx
                )
                filesToSign.append(fileToSign)
                orderedItems.append(SignedFileItem(fileToSign))
                numSignedFiles += 1
              }
            case (r: RemoveFile, idx) if r.dataChange && earlyReturnToken.isEmpty =>
              // Check if we've reached page size limit
              if (pageSizeOpt.contains(numSignedFiles)) {
                earlyReturnToken = Some(tokenGenerator(v, idx))
              } else {
                val fileToSign = FileToSign(
                  absolutePath(deltaLog.dataPath, r.path),
                  Right(r),
                  v,
                  ts,
                  idx
                )
                filesToSign.append(fileToSign)
                orderedItems.append(SignedFileItem(fileToSign))
                numSignedFiles += 1
              }
            case (p: Protocol, _) if earlyReturnToken.isEmpty =>
              assertProtocolRead(p)
            case (m: Metadata, _) if earlyReturnToken.isEmpty =>
              if (v > startingVersion) {
                orderedItems.append(MetadataItem(m, v))
              }
            case _ => ()
          }
        }
      }

    val tSign = System.nanoTime()
    val signedUrls = parallelSign(filesToSign.map(_.path).toSeq)
    val signingNs = System.nanoTime() - tSign

    val pathToSignedUrl = filesToSign.zip(signedUrls).toMap
    val actions = ListBuffer[Object]()

    orderedItems.foreach {
      case SignedFileItem(fileToSign) =>
        val preSignedUrl = pathToSignedUrl(fileToSign)
        minUrlExpirationTimestamp =
          minUrlExpirationTimestamp.min(preSignedUrl.expirationTimestamp)

        fileToSign.action match {
          case Left(addFile) =>
            actions.append(
              getResponseAddFile(
                addFile,
                preSignedUrl,
                fileToSign.version,
                fileToSign.timestamp.getTime,
                responseFormat,
                true
              )
            )
          case Right(removeFile) =>
            actions.append(
              getResponseRemoveFile(
                removeFile,
                preSignedUrl,
                fileToSign.version,
                fileToSign.timestamp.getTime,
                responseFormat
              )
            )
        }
      case MetadataItem(m, v) =>
        actions.append(
          getResponseMetadata(
            m,
            Some(v),
            responseFormat
          )
        )
    }

    if (earlyReturnToken.isDefined) {
      actions.append(getEndStreamAction(earlyReturnToken.get, minUrlExpirationTimestamp))
      val scanWallNs = System.nanoTime() - tScan
      return (
        actions.toSeq,
        timestampIndexNs,
        scanWallNs - signingNs,
        signingNs,
        versionsIterated,
        start,
        end)
    }

    val scanWallNs = System.nanoTime() - tScan
    val changeReplayNs = scanWallNs - signingNs
    // Return an `endStreamAction` object only when `maxFiles` or includeEndStreamAction is
    // specified for backwards compatibility.
    if (maxFilesOpt.isDefined || includeEndStreamAction) {
      actions.append(getEndStreamAction(null, minUrlExpirationTimestamp))
    }
    (
      actions.toSeq,
      timestampIndexNs,
      changeReplayNs,
      signingNs,
      versionsIterated,
      start,
      end)
  }

  def queryCDF(
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean = false,
      maxFiles: Option[Int],
      pageToken: Option[String],
      responseFormatSet: Set[String] = Set(DeltaSharedTable.RESPONSE_FORMAT_PARQUET),
      includeEndStreamAction: Boolean,
      deltaLogUpdateNs: Long = 0L,
      requestTimeoutSecondsForLogging: Option[Long] = None): QueryResult = withClassLoader {
    // Step 1: validate pageToken if it's specified
    lazy val queryParamChecksum = computeChecksum(
      QueryParamChecksum(
        version = None,
        timestamp = None,
        startingVersion = cdfOptions.get(DeltaDataSource.CDF_START_VERSION_KEY).map(_.toLong),
        startingTimestamp = cdfOptions.get(DeltaDataSource.CDF_START_TIMESTAMP_KEY),
        endingVersion = cdfOptions.get(DeltaDataSource.CDF_END_VERSION_KEY).map(_.toLong),
        endingTimestamp = cdfOptions.get(DeltaDataSource.CDF_END_TIMESTAMP_KEY),
        predicateHints = Nil,
        jsonPredicateHints = None,
        limitHint = None,
        includeHistoricalMetadata = Some(includeHistoricalMetadata)
      )
    )
    val pageTokenOpt = pageToken.map(decodeAndValidatePageToken(_, queryParamChecksum))

    // Step 2: validate cdfOptions
    val cdcReader = new DeltaSharingCDCReader(deltaLog, conf)
    // For subsequent page calls, instead of using the current latestVersion, use latestVersion in
    // the pageToken (which is equal to the latestVersion when the first page call is received),
    // in case the latestVersion changes after the first page call.
    val latestVersion = pageTokenOpt.map(_.getLatestVersion).getOrElse(tableVersion)
    val (start, end) = cdcReader.validateCdfOptions(
      cdfOptions, latestVersion, tableConfig.startVersion)

    val cdfStart = pageTokenOpt.map(_.getStartingVersion).getOrElse(start)
    val cdfEnd = pageTokenOpt.map(_.getEndingVersion).getOrElse(end).min(latestVersion)

    // Step 3: get Protocol and Metadata
    val tProtocolSnapshot = System.nanoTime()
    val snapshot = if (includeHistoricalMetadata) {
      deltaLog.getSnapshotForVersionAsOf(start)
    } else {
      deltaLog.getSnapshotForVersionAsOf(latestVersion)
    }
    val actions = ListBuffer[Object]()
    // If the client accept parquet format and it's a basic table, return as parquet format.
    val responseFormat = if (snapshot.protocolScala.minReaderVersion == 1 &&
      responseFormatSet.contains(DeltaSharedTable.RESPONSE_FORMAT_PARQUET)) {
      DeltaSharedTable.RESPONSE_FORMAT_PARQUET
    } else {
      DeltaSharedTable.RESPONSE_FORMAT_DELTA
    }
    actions.append(getResponseProtocol(snapshot.protocolScala, responseFormat))
    actions.append(
      getResponseMetadata(
        snapshot.metadataScala,
        Some(snapshot.version),
        responseFormat
      )
    )
    val protocolSnapshotNs = System.nanoTime() - tProtocolSnapshot

    // Step 4: get files
    // Enforce page size only when `maxFiles` is specified for backwards compatibility.
    val pageSizeOpt = maxFiles.map(_.min(queryTablePageSizeLimit))
    val tokenGenerator = { (v: Long, idx: Int) =>
      DeltaSharedTable.encodeToken(
        QueryTablePageToken(
          id = Some(tableConfig.id),
          startingVersion = Some(v),
          endingVersion = Some(pageTokenOpt.map(_.getEndingVersion).getOrElse(end)),
          latestVersion = Some(latestVersion),
          checksum = Some(queryParamChecksum),
          startingActionIndex = Some(idx),
          expirationTimestamp = Some(System.currentTimeMillis() + queryTablePageTokenTtlMs)
        )
      )
    }
    var minUrlExpirationTimestamp = Long.MaxValue
    var numSignedFiles = 0

    // Track actions that need signing and non-file actions, preserving order
    sealed trait CdfAction
    case class AddCDCFileAction(c: AddCDCFile) extends CdfAction
    case class AddFileAction(a: AddFile) extends CdfAction
    case class RemoveFileAction(r: RemoveFile) extends CdfAction

    case class CdfFileToSign(
      path: Path,
      action: CdfAction,
      version: Long,
      timestamp: java.sql.Timestamp,
      idx: Int)

    sealed trait CdfActionItem
    case class CdfSignedFileItem(fileToSign: CdfFileToSign) extends CdfActionItem
    case class CdfMetadataItem(m: Metadata, v: Long) extends CdfActionItem

    val filesToSign = ListBuffer[CdfFileToSign]()
    val orderedItems = ListBuffer[CdfActionItem]()
    var earlyReturnToken: Option[String] = None

    // We use (start, end) from the page token instead of the original request because:
    // - Versions that are processed in previous pages can be skipped.
    // - Versions that are committed after the first page call should be ignored, especially
    //   when the endingVersion is not specified and resolved to latestVersion.
    val replayOut = cdcReader.queryCDF(
      cdfStart,
      cdfEnd,
      latestVersion,
      includeHistoricalMetadata
    )
    val versionsIterated = replayOut.specs.length

    def cdfPartialTimings(signingNs: Long): CdfQueryTimings = {
      CdfQueryTimings(
        cdfStartVersion = cdfStart,
        cdfEndVersion = cdfEnd,
        versionsIterated = versionsIterated,
        deltaLogUpdateNs = deltaLogUpdateNs,
        protocolSnapshotNs = protocolSnapshotNs,
        getChangesNs = replayOut.getChangesMaterializeNs,
        timestampIndexNs = replayOut.timestampIndexNs,
        cdcSpecBuildNs = replayOut.cdcSpecBuildNs,
        signingNs = signingNs)
    }
    def cdfEarlyReturn(pt: CdfQueryTimings): QueryResult = {
      warnIfNearRequestTimeout(requestTimeoutSecondsForLogging, cdfInternalWorkNs(pt), "cdf")
      QueryResult(start, actions.toSeq, responseFormat, Some(CdfTimings(pt)))
    }

    // First pass: collect files to sign and non-file actions, respecting page size
    replayOut.specs.foreach { cdcDataSpec =>
      if (earlyReturnToken.isEmpty) {
        val v = cdcDataSpec.version
        val ts = cdcDataSpec.timestamp
        var indexedActions = cdcDataSpec.actions.zipWithIndex
        if (pageTokenOpt.exists(_.getStartingVersion == v)) {
          // Skip actions that are already processed in previous pages
          indexedActions = indexedActions.drop(pageTokenOpt.get.getStartingActionIndex)
        }
        indexedActions.foreach {
          case (m: Metadata, _) if earlyReturnToken.isEmpty =>
            orderedItems.append(CdfMetadataItem(m, v))
          case (c: AddCDCFile, idx) if earlyReturnToken.isEmpty =>
            // Check if we've reached page size limit
            if (pageSizeOpt.contains(numSignedFiles)) {
              earlyReturnToken = Some(tokenGenerator(v, idx))
            } else {
              val fileToSign = CdfFileToSign(
                absolutePath(deltaLog.dataPath, c.path),
                AddCDCFileAction(c),
                v,
                ts,
                idx
              )
              filesToSign.append(fileToSign)
              orderedItems.append(CdfSignedFileItem(fileToSign))
              numSignedFiles += 1
            }
          case (a: AddFile, idx) if earlyReturnToken.isEmpty =>
            // Check if we've reached page size limit
            if (pageSizeOpt.contains(numSignedFiles)) {
              earlyReturnToken = Some(tokenGenerator(v, idx))
            } else {
              val fileToSign = CdfFileToSign(
                absolutePath(deltaLog.dataPath, a.path),
                AddFileAction(a),
                v,
                ts,
                idx
              )
              filesToSign.append(fileToSign)
              orderedItems.append(CdfSignedFileItem(fileToSign))
              numSignedFiles += 1
            }
          case (r: RemoveFile, idx) if earlyReturnToken.isEmpty =>
            // Check if we've reached page size limit
            if (pageSizeOpt.contains(numSignedFiles)) {
              earlyReturnToken = Some(tokenGenerator(v, idx))
            } else {
              val fileToSign = CdfFileToSign(
                absolutePath(deltaLog.dataPath, r.path),
                RemoveFileAction(r),
                v,
                ts,
                idx
              )
              filesToSign.append(fileToSign)
              orderedItems.append(CdfSignedFileItem(fileToSign))
              numSignedFiles += 1
            }
          case _ => ()
        }
      }
    }

    // Second pass: sign all paths in parallel
    val tSign = System.nanoTime()
    val signedUrls = parallelSign(filesToSign.map(_.path).toSeq)
    val signingNs = System.nanoTime() - tSign

    // Third pass: build response actions with signed URLs
    val pathToSignedUrl = filesToSign.zip(signedUrls).toMap

    orderedItems.foreach {
      case CdfSignedFileItem(fileToSign) =>
        val preSignedUrl = pathToSignedUrl(fileToSign)
        minUrlExpirationTimestamp =
          minUrlExpirationTimestamp.min(preSignedUrl.expirationTimestamp)

        fileToSign.action match {
          case AddCDCFileAction(c) =>
            actions.append(
              getResponseAddCDCFile(
                c,
                preSignedUrl,
                fileToSign.version,
                fileToSign.timestamp.getTime,
                responseFormat
              )
            )
          case AddFileAction(a) =>
            actions.append(
              getResponseAddFile(
                a,
                preSignedUrl,
                fileToSign.version,
                fileToSign.timestamp.getTime,
                responseFormat,
                returnAddFileForCDF = true
              )
            )
          case RemoveFileAction(r) =>
            actions.append(
              getResponseRemoveFile(
                r,
                preSignedUrl,
                fileToSign.version,
                fileToSign.timestamp.getTime,
                responseFormat
              )
            )
        }
      case CdfMetadataItem(m, v) =>
        actions.append(
          getResponseMetadata(
            m,
            Some(v),
            responseFormat
          )
        )
    }

    // Handle early return if page size was exceeded
    if (earlyReturnToken.isDefined) {
      actions.append(getEndStreamAction(earlyReturnToken.get, minUrlExpirationTimestamp))
      return cdfEarlyReturn(cdfPartialTimings(signingNs))
    }
    // Return an `endStreamAction` object only when `maxFiles` is specified for
    // backwards compatibility.
    if (maxFiles.isDefined || includeEndStreamAction) {
      actions.append(getEndStreamAction(null, minUrlExpirationTimestamp))
    }
    val cdfTimings = cdfPartialTimings(signingNs)
    warnIfNearRequestTimeout(requestTimeoutSecondsForLogging, cdfInternalWorkNs(cdfTimings), "cdf")
    QueryResult(start, actions.toSeq, responseFormat, Some(CdfTimings(cdfTimings)))
  }

  private def cdfInternalWorkNs(t: CdfQueryTimings): Long = {
    t.deltaLogUpdateNs + t.protocolSnapshotNs + t.cdfReplayNs + t.signingNs
  }

  private def tableInternalWorkNs(t: TableQueryTimings): Long = {
    t.deltaLogUpdateNs + t.snapshotResolveNs + t.replayOrPrepareNs + t.signingNs
  }

  private def warnIfNearRequestTimeout(
      requestTimeoutSecondsForLogging: Option[Long],
      observedWorkNs: Long,
      kind: String): Unit = {
    requestTimeoutSecondsForLogging.foreach { sec =>
      val limNs = sec * 1000000000L
      if (limNs > 0 && observedWorkNs > (limNs * 3) / 4) {
        logger.warn(
          s"$kind path internal work ~${observedWorkNs / 1000000}ms " +
            s"is above 75% of request timeout (${sec}s) for table ${tableConfig.getName}")
      }
    }
  }

  def update(): Unit = withClassLoader {
    deltaLog.update()
  }

  private def assertProtocolRead(protocol: Protocol): Unit = {
    if (protocol.minReaderVersion > model.Action.maxReaderVersion) {
      val e = new DeltaErrors.InvalidProtocolVersionException(Protocol(
        model.Action.maxReaderVersion, model.Action.maxWriterVersion), protocol)
      throw new DeltaSharingUnsupportedOperationException(e.getMessage)
    }
  }

  private def getMetadataConfiguration(tableConf: Map[String, String]): Map[String, String ] = {
    if (tableConfig.historyShared &&
      tableConf.getOrElse("delta.enableChangeDataFeed", "false") == "true") {
      Map("enableChangeDataFeed" -> "true")
    } else {
      Map.empty
    }
  }

  private def cleanUpTableSchema(schemaString: String): String = {
    StructType(DataType.fromJson(schemaString).asInstanceOf[StructType].map { field =>
      val newMetadata = new MetadataBuilder()
      // Only keep the column comment
      if (field.metadata.contains("comment")) {
        newMetadata.putString("comment", field.metadata.getString("comment"))
      }
      field.copy(metadata = newMetadata.build())
    }).json
  }

  private def absolutePath(path: Path, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      throw new IllegalStateException("table containing absolute paths cannot be shared")
    } else {
      new Path(path, p)
    }
  }

  private def computeChecksum(queryParamChecksum: QueryParamChecksum): String = {
    DigestUtils.sha256Hex(JsonUtils.toJson(queryParamChecksum))
  }

  private def decodeAndValidatePageToken(
      tokenStr: String,
      expectedChecksum: String): QueryTablePageToken = {
    val token = try {
      DeltaSharedTable.decodeToken[QueryTablePageToken](tokenStr)
    } catch {
      case NonFatal(_) =>
        throw new DeltaSharingIllegalArgumentException(
          s"Error decoding the page token: $tokenStr."
        )
    }
    if (token.getExpirationTimestamp < System.currentTimeMillis()) {
      throw new DeltaSharingIllegalArgumentException(
        "The page token has expired. Please restart the query."
      )
    }
    if (token.getId != tableConfig.id) {
      throw new DeltaSharingIllegalArgumentException(
        "The table specified in the page token does not match the table being queried."
      )
    }
    if (token.getChecksum != expectedChecksum) {
      throw new DeltaSharingIllegalArgumentException(
        """Query parameter mismatch detected for the next page query. The query parameter
          |cannot change when querying the next page results.""".stripMargin
      )
    }
    token
  }

  private def decodeAndValidateRefreshToken(tokenStr: String): RefreshToken = {
    val token = try {
      DeltaSharedTable.decodeToken[RefreshToken](tokenStr)
    } catch {
      case NonFatal(_) =>
        throw new DeltaSharingIllegalArgumentException(
          s"Error decoding refresh token: $tokenStr."
        )
    }
    if (token.getExpirationTimestamp < System.currentTimeMillis()) {
      throw new DeltaSharingIllegalArgumentException(
        "The refresh token has expired. Please restart the query."
      )
    }
    if (token.getId != tableConfig.id) {
      throw new DeltaSharingIllegalArgumentException(
        "The table specified in the refresh token does not match the table being queried."
      )
    }
    token
  }
}

object DeltaSharedTable {
  val RESPONSE_FORMAT_PARQUET = "parquet"
  val RESPONSE_FORMAT_DELTA = "delta"

  // Size of the shared signing thread pool, configurable via ServerConfig#signingThreadPoolSize.
  // Must be set (via `configureSigningThreadPoolSize`) before the pool is first used, since the
  // pool itself is created lazily on first access.
  private val signingThreadPoolSize = new java.util.concurrent.atomic.AtomicInteger(32)

  def configureSigningThreadPoolSize(size: Int): Unit = {
    require(size > 0, s"signingThreadPoolSize must be positive, got $size")
    signingThreadPoolSize.set(size)
  }

  // Shared, bounded thread pool for parallel file signing across all tables/requests.
  private lazy val signingExecutionContext: ExecutionContext = ExecutionContext.fromExecutorService(
    java.util.concurrent.Executors.newFixedThreadPool(signingThreadPoolSize.get()))

  private def encodeToken[T <: GeneratedMessage](token: T): String = {
    Base64.getUrlEncoder.encodeToString(token.toByteArray)
  }

  private def decodeToken[T <: GeneratedMessage](tokenStr: String)(
    implicit protoCompanion: GeneratedMessageCompanion[T]): T = {
    protoCompanion.parseFrom(Base64.getUrlDecoder.decode(tokenStr))
  }
}
