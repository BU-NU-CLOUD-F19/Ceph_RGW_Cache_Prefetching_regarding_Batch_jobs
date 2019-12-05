/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import java.util.UUID

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.GetTableTypesOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Spark's own GetTableTypesOperation
 *
 * @param sqlContext SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 */
private[hive] class SparkGetTableTypesOperation(
    sqlContext: SQLContext,
    parentSession: HiveSession)
  extends GetTableTypesOperation(parentSession) with SparkMetadataOperationUtils with Logging {

  private var statementId: String = _

  override def close(): Unit = {
    super.close()
    HiveThriftServer2.listener.onOperationClosed(statementId)
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    val logMsg = "Listing table types"
    logInfo(s"$logMsg with $statementId")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    if (isAuthV2Enabled) {
      authorizeMetaGets(HiveOperationType.GET_TABLETYPES, null)
    }

    HiveThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    try {
      val tableTypes = CatalogTableType.tableTypes.map(tableTypeString).toSet
      tableTypes.foreach { tableType =>
        rowSet.addRow(Array[AnyRef](tableType))
      }
      setState(OperationState.FINISHED)
    } catch {
      case e: Throwable =>
        logError(s"Error executing get table types operation with $statementId", e)
        setState(OperationState.ERROR)
        e match {
          case hiveException: HiveSQLException =>
            HiveThriftServer2.listener.onStatementError(
              statementId, hiveException.getMessage, SparkUtils.exceptionString(hiveException))
            throw hiveException
          case _ =>
            val root = ExceptionUtils.getRootCause(e)
            HiveThriftServer2.listener.onStatementError(
              statementId, root.getMessage, SparkUtils.exceptionString(root))
            throw new HiveSQLException("Error getting table types: " + root.toString, root)
        }
    }
    HiveThriftServer2.listener.onStatementFinish(statementId)
  }
}
