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

package org.apache.spark.sql.execution.datasources.orc

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.orc.{OrcFile, Reader, TypeDescription, Writer}

import org.apache.spark.{SPARK_VERSION_SHORT, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SPARK_VERSION_METADATA_KEY, SparkSession}
import org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.execution.datasources.SchemaMergeUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.{ThreadUtils, Utils}

object OrcUtils extends Logging {

  // The extensions for ORC compression codecs
  val extensionsForCompressionCodecNames = Map(
    "NONE" -> "",
    "SNAPPY" -> ".snappy",
    "ZLIB" -> ".zlib",
    "LZO" -> ".lzo")

  def listOrcFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    val paths = SparkHadoopUtil.get.listLeafStatuses(fs, origPath)
      .filterNot(_.isDirectory)
      .map(_.getPath)
      .filterNot(_.getName.startsWith("_"))
      .filterNot(_.getName.startsWith("."))
    paths
  }

  def readSchema(file: Path, conf: Configuration, ignoreCorruptFiles: Boolean)
      : Option[TypeDescription] = {
    val fs = file.getFileSystem(conf)
    val readerOptions = OrcFile.readerOptions(conf).filesystem(fs)
    try {
      val schema = Utils.tryWithResource(OrcFile.createReader(file, readerOptions)) { reader =>
        reader.getSchema
      }
      if (schema.getFieldNames.size == 0) {
        None
      } else {
        Some(schema)
      }
    } catch {
      case e: org.apache.orc.FileFormatException =>
        if (ignoreCorruptFiles) {
          logWarning(s"Skipped the footer in the corrupted file: $file", e)
          None
        } else {
          throw new SparkException(s"Could not read footer for file: $file", e)
        }
    }
  }

  def readSchema(sparkSession: SparkSession, files: Seq[FileStatus])
      : Option[StructType] = {
    val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
    val conf = sparkSession.sessionState.newHadoopConf()
    files.toIterator.map(file => readSchema(file.getPath, conf, ignoreCorruptFiles)).collectFirst {
      case Some(schema) =>
        logDebug(s"Reading schema from file $files, got Hive schema string: $schema")
        CatalystSqlParser.parseDataType(schema.toString).asInstanceOf[StructType]
    }
  }

  /**
   * Reads ORC file schemas in multi-threaded manner, using native version of ORC.
   * This is visible for testing.
   */
  def readOrcSchemasInParallel(
    files: Seq[FileStatus], conf: Configuration, ignoreCorruptFiles: Boolean): Seq[StructType] = {
    ThreadUtils.parmap(files, "readingOrcSchemas", 8) { currentFile =>
      OrcUtils.readSchema(currentFile.getPath, conf, ignoreCorruptFiles)
        .map(s => CatalystSqlParser.parseDataType(s.toString).asInstanceOf[StructType])
    }.flatten
  }

  def inferSchema(sparkSession: SparkSession, files: Seq[FileStatus], options: Map[String, String])
    : Option[StructType] = {
    val orcOptions = new OrcOptions(options, sparkSession.sessionState.conf)
    if (orcOptions.mergeSchema) {
      SchemaMergeUtils.mergeSchemasInParallel(
        sparkSession, files, OrcUtils.readOrcSchemasInParallel)
    } else {
      OrcUtils.readSchema(sparkSession, files)
    }
  }

  /**
   * Returns the requested column ids from the given ORC file. Column id can be -1, which means the
   * requested column doesn't exist in the ORC file. Returns None if the given ORC file is empty.
   */
  def requestedColumnIds(
      isCaseSensitive: Boolean,
      dataSchema: StructType,
      requiredSchema: StructType,
      reader: Reader,
      conf: Configuration): Option[Array[Int]] = {
    val orcFieldNames = reader.getSchema.getFieldNames.asScala
    if (orcFieldNames.isEmpty) {
      // SPARK-8501: Some old empty ORC files always have an empty schema stored in their footer.
      None
    } else {
      if (orcFieldNames.forall(_.startsWith("_col"))) {
        // This is a ORC file written by Hive, no field names in the physical schema, assume the
        // physical schema maps to the data scheme by index.
        assert(orcFieldNames.length <= dataSchema.length, "The given data schema " +
          s"${dataSchema.catalogString} has less fields than the actual ORC physical schema, " +
          "no idea which columns were dropped, fail to read.")
        Some(requiredSchema.fieldNames.map { name =>
          val index = dataSchema.fieldIndex(name)
          if (index < orcFieldNames.length) {
            index
          } else {
            -1
          }
        })
      } else {
        if (isCaseSensitive) {
          Some(requiredSchema.fieldNames.zipWithIndex.map { case (name, idx) =>
            if (orcFieldNames.indexWhere(caseSensitiveResolution(_, name)) != -1) {
              idx
            } else {
              -1
            }
          })
        } else {
          // Do case-insensitive resolution only if in case-insensitive mode
          val caseInsensitiveOrcFieldMap = orcFieldNames.groupBy(_.toLowerCase(Locale.ROOT))
          Some(requiredSchema.fieldNames.zipWithIndex.map { case (requiredFieldName, idx) =>
            caseInsensitiveOrcFieldMap
              .get(requiredFieldName.toLowerCase(Locale.ROOT))
              .map { matchedOrcFields =>
                if (matchedOrcFields.size > 1) {
                  // Need to fail if there is ambiguity, i.e. more than one field is matched.
                  val matchedOrcFieldsString = matchedOrcFields.mkString("[", ", ", "]")
                  reader.close()
                  throw new RuntimeException(s"""Found duplicate field(s) "$requiredFieldName": """
                    + s"$matchedOrcFieldsString in case-insensitive mode")
                } else {
                  idx
                }
              }.getOrElse(-1)
          })
        }
      }
    }
  }

  /**
   * Add a metadata specifying Spark version.
   */
  def addSparkVersionMetadata(writer: Writer): Unit = {
    writer.addUserMetadata(SPARK_VERSION_METADATA_KEY, UTF_8.encode(SPARK_VERSION_SHORT))
  }

  /**
   * Given a `StructType` object, this methods converts it to corresponding string representation
   * in ORC.
   */
  def orcTypeDescriptionString(dt: DataType): String = dt match {
    case s: StructType =>
      val fieldTypes = s.fields.map { f =>
        s"${quoteIdentifier(f.name)}:${orcTypeDescriptionString(f.dataType)}"
      }
      s"struct<${fieldTypes.mkString(",")}>"
    case a: ArrayType =>
      s"array<${orcTypeDescriptionString(a.elementType)}>"
    case m: MapType =>
      s"map<${orcTypeDescriptionString(m.keyType)},${orcTypeDescriptionString(m.valueType)}>"
    case _ => dt.catalogString
  }
}
