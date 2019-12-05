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

package org.apache.spark.sql.execution.datasources.noop

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, SupportsTruncate, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * This is no-op datasource. It does not do anything besides consuming its input.
 * This can be useful for benchmarking or to cache data without any additional overhead.
 */
class NoopDataSource extends TableProvider with DataSourceRegister {
  override def shortName(): String = "noop"
  override def getTable(options: CaseInsensitiveStringMap): Table = NoopTable
}

private[noop] object NoopTable extends Table with SupportsWrite {
  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = NoopWriteBuilder
  override def name(): String = "noop-table"
  override def schema(): StructType = new StructType()
  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.TRUNCATE,
      TableCapability.ACCEPT_ANY_SCHEMA).asJava
  }
}

private[noop] object NoopWriteBuilder extends WriteBuilder with SupportsTruncate {
  override def truncate(): WriteBuilder = this
  override def buildForBatch(): BatchWrite = NoopBatchWrite
  override def buildForStreaming(): StreamingWrite = NoopStreamingWrite
}

private[noop] object NoopBatchWrite extends BatchWrite {
  override def createBatchWriterFactory(): DataWriterFactory = NoopWriterFactory
  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

private[noop] object NoopWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = NoopWriter
}

private[noop] object NoopWriter extends DataWriter[InternalRow] {
  override def write(record: InternalRow): Unit = {}
  override def commit(): WriterCommitMessage = null
  override def abort(): Unit = {}
}

private[noop] object NoopStreamingWrite extends StreamingWrite {
  override def createStreamingWriterFactory(): StreamingDataWriterFactory =
    NoopStreamingDataWriterFactory
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

private[noop] object NoopStreamingDataWriterFactory extends StreamingDataWriterFactory {
  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = NoopWriter
}
