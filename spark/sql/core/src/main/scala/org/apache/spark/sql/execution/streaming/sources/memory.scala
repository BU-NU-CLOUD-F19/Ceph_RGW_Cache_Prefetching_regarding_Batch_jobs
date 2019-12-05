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

package org.apache.spark.sql.execution.streaming.sources

import java.util
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory, SupportsTruncate, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A sink that stores the results in memory. This [[Sink]] is primarily intended for use in unit
 * tests and does not provide durability.
 */
class MemorySink extends Table with SupportsWrite with Logging {

  override def name(): String = "MemorySink"

  override def schema(): StructType = StructType(Nil)

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.STREAMING_WRITE).asJava
  }

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder = {
    new WriteBuilder with SupportsTruncate {
      private var needTruncate: Boolean = false
      private var inputSchema: StructType = _

      override def truncate(): WriteBuilder = {
        this.needTruncate = true
        this
      }

      override def withInputDataSchema(schema: StructType): WriteBuilder = {
        this.inputSchema = schema
        this
      }

      override def buildForStreaming(): StreamingWrite = {
        new MemoryStreamingWrite(MemorySink.this, inputSchema, needTruncate)
      }
    }
  }

  private case class AddedData(batchId: Long, data: Array[Row])

  /** An order list of batches that have been written to this [[Sink]]. */
  @GuardedBy("this")
  private val batches = new ArrayBuffer[AddedData]()

  /** Returns all rows that are stored in this [[Sink]]. */
  def allData: Seq[Row] = synchronized {
    batches.flatMap(_.data)
  }

  def latestBatchId: Option[Long] = synchronized {
    batches.lastOption.map(_.batchId)
  }

  def latestBatchData: Seq[Row] = synchronized {
    batches.lastOption.toSeq.flatten(_.data)
  }

  def dataSinceBatch(sinceBatchId: Long): Seq[Row] = synchronized {
    batches.filter(_.batchId > sinceBatchId).flatMap(_.data)
  }

  def toDebugString: String = synchronized {
    batches.map { case AddedData(batchId, data) =>
      val dataStr = try data.mkString(" ") catch {
        case NonFatal(e) => "[Error converting to string]"
      }
      s"$batchId: $dataStr"
    }.mkString("\n")
  }

  def write(batchId: Long, needTruncate: Boolean, newRows: Array[Row]): Unit = {
    val notCommitted = synchronized {
      latestBatchId.isEmpty || batchId > latestBatchId.get
    }
    if (notCommitted) {
      logDebug(s"Committing batch $batchId to $this")
      val rows = AddedData(batchId, newRows)
      if (needTruncate) {
        synchronized {
          batches.clear()
          batches += rows
        }
      } else {
        synchronized { batches += rows }
      }
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }

  def clear(): Unit = synchronized {
    batches.clear()
  }

  override def toString(): String = "MemorySink"
}

case class MemoryWriterCommitMessage(partition: Int, data: Seq[Row])
  extends WriterCommitMessage {}

class MemoryStreamingWrite(
    val sink: MemorySink, schema: StructType, needTruncate: Boolean)
  extends StreamingWrite {

  override def createStreamingWriterFactory: MemoryWriterFactory = {
    MemoryWriterFactory(schema)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    val newRows = messages.flatMap {
      case message: MemoryWriterCommitMessage => message.data
    }
    sink.write(epochId, needTruncate, newRows)
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    // Don't accept any of the new input.
  }
}

case class MemoryWriterFactory(schema: StructType)
  extends DataWriterFactory with StreamingDataWriterFactory {

  override def createWriter(
      partitionId: Int,
      taskId: Long): DataWriter[InternalRow] = {
    new MemoryDataWriter(partitionId, schema)
  }

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    createWriter(partitionId, taskId)
  }
}

class MemoryDataWriter(partition: Int, schema: StructType)
  extends DataWriter[InternalRow] with Logging {

  private val data = mutable.Buffer[Row]()

  private val encoder = RowEncoder(schema).resolveAndBind()

  override def write(row: InternalRow): Unit = {
    data.append(encoder.fromRow(row))
  }

  override def commit(): MemoryWriterCommitMessage = {
    val msg = MemoryWriterCommitMessage(partition, data.clone())
    data.clear()
    msg
  }

  override def abort(): Unit = {}
}


/**
 * Used to query the data that has been written into a [[MemorySink]].
 */
case class MemoryPlan(sink: MemorySink, override val output: Seq[Attribute]) extends LeafNode {
  private val sizePerRow = EstimationUtils.getSizePerRow(output)

  override def computeStats(): Statistics = Statistics(sizePerRow * sink.allData.size)
}

