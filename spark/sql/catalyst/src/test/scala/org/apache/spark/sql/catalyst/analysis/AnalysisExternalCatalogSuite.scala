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

package org.apache.spark.sql.catalyst.analysis

import java.io.File
import java.net.URI

import org.mockito.Mockito._
import org.scalatest.Matchers

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType, ExternalCatalog, InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class AnalysisExternalCatalogSuite extends AnalysisTest with Matchers {
  private def getAnalyzer(externCatalog: ExternalCatalog, databasePath: File): Analyzer = {
    val conf = new SQLConf()
    val catalog = new SessionCatalog(externCatalog, FunctionRegistry.builtin, conf)
    catalog.createDatabase(
      CatalogDatabase("default", "", databasePath.toURI, Map.empty),
      ignoreIfExists = false)
    catalog.createTable(
      CatalogTable(
        TableIdentifier("t1", Some("default")),
        CatalogTableType.MANAGED,
        CatalogStorageFormat.empty,
        StructType(Seq(StructField("a", IntegerType, nullable = true)))),
      ignoreIfExists = false)
    new Analyzer(catalog, conf)
  }

  test("query builtin functions don't call the external catalog") {
    withTempDir { tempDir =>
      val inMemoryCatalog = new InMemoryCatalog
      val catalog = spy(inMemoryCatalog)
      val analyzer = getAnalyzer(catalog, tempDir)
      reset(catalog)
      val testRelation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())
      val func =
        Alias(UnresolvedFunction("sum", Seq(UnresolvedAttribute("a")), isDistinct = false), "s")()
      val plan = Project(Seq(func), testRelation)
      analyzer.execute(plan)
      verifyZeroInteractions(catalog)
    }
  }

  test("check the existence of builtin functions don't call the external catalog") {
    withTempDir { tempDir =>
      val inMemoryCatalog = new InMemoryCatalog
      val externCatalog = spy(inMemoryCatalog)
      val catalog = new SessionCatalog(externCatalog, FunctionRegistry.builtin, conf)
      catalog.createDatabase(
        CatalogDatabase("default", "", new URI(tempDir.toString), Map.empty),
        ignoreIfExists = false)
      reset(externCatalog)
      catalog.functionExists(FunctionIdentifier("sum"))
      verifyZeroInteractions(externCatalog)
    }
  }

}
