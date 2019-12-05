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

package org.apache.spark.sql.kafka010

import java.{util => ju}
import java.io._
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Network.NETWORK_TIMEOUT
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.kafka010.KafkaSource._
import org.apache.spark.sql.kafka010.KafkaSourceProvider._
import org.apache.spark.sql.types._

/**
 * A [[Source]] that reads data from Kafka using the following design.
 *
 * - The [[KafkaSourceOffset]] is the custom [[Offset]] defined for this source that contains
 *   a map of TopicPartition -> offset. Note that this offset is 1 + (available offset). For
 *   example if the last record in a Kafka topic "t", partition 2 is offset 5, then
 *   KafkaSourceOffset will contain TopicPartition("t", 2) -> 6. This is done keep it consistent
 *   with the semantics of `KafkaConsumer.position()`.
 *
 * - The [[KafkaSource]] written to do the following.
 *
 *  - As soon as the source is created, the pre-configured [[KafkaOffsetReader]]
 *    is used to query the initial offsets that this source should
 *    start reading from. This is used to create the first batch.
 *
 *   - `getOffset()` uses the [[KafkaOffsetReader]] to query the latest
 *      available offsets, which are returned as a [[KafkaSourceOffset]].
 *
 *   - `getBatch()` returns a DF that reads from the 'start offset' until the 'end offset' in
 *     for each partition. The end offset is excluded to be consistent with the semantics of
 *     [[KafkaSourceOffset]] and `KafkaConsumer.position()`.
 *
 *   - The DF returned is based on [[KafkaSourceRDD]] which is constructed such that the
 *     data from Kafka topic + partition is consistently read by the same executors across
 *     batches, and cached KafkaConsumers in the executors can be reused efficiently. See the
 *     docs on [[KafkaSourceRDD]] for more details.
 *
 * Zero data lost is not guaranteed when topics are deleted. If zero data lost is critical, the user
 * must make sure all messages in a topic have been processed when deleting a topic.
 *
 * There is a known issue caused by KAFKA-1894: the query using KafkaSource maybe cannot be stopped.
 * To avoid this issue, you should make sure stopping the query before stopping the Kafka brokers
 * and not use wrong broker addresses.
 */
private[kafka010] class KafkaSource(
    sqlContext: SQLContext,
    kafkaReader: KafkaOffsetReader,
    executorKafkaParams: ju.Map[String, Object],
    sourceOptions: CaseInsensitiveMap[String],
    metadataPath: String,
    startingOffsets: KafkaOffsetRangeLimit,
    failOnDataLoss: Boolean)
  extends Source with Logging {

  private val sc = sqlContext.sparkContext

  private val pollTimeoutMs =
    sourceOptions.getOrElse(CONSUMER_POLL_TIMEOUT, (sc.conf.get(NETWORK_TIMEOUT) * 1000L).toString)
      .toLong

  private val maxOffsetsPerTrigger =
    sourceOptions.get(MAX_OFFSET_PER_TRIGGER).map(_.toLong)

  private val includeHeaders =
    sourceOptions.getOrElse(INCLUDE_HEADERS, "false").toBoolean

  /**
   * Lazily initialize `initialPartitionOffsets` to make sure that `KafkaConsumer.poll` is only
   * called in StreamExecutionThread. Otherwise, interrupting a thread while running
   * `KafkaConsumer.poll` may hang forever (KAFKA-1894).
   */
  private lazy val initialPartitionOffsets = {
    val metadataLog = new KafkaSourceInitialOffsetWriter(sqlContext.sparkSession, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsetRangeLimit => KafkaSourceOffset(kafkaReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit => KafkaSourceOffset(kafkaReader.fetchLatestOffsets(None))
        case SpecificOffsetRangeLimit(p) => kafkaReader.fetchSpecificOffsets(p, reportDataLoss)
        case SpecificTimestampRangeLimit(p) =>
          kafkaReader.fetchSpecificTimestampBasedOffsets(p, failsOnNoMatchingOffset = true)
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.partitionToOffsets
  }

  private var currentPartitionOffsets: Option[Map[TopicPartition, Long]] = None

  private val converter = new KafkaRecordToRowConverter()

  override def schema: StructType = KafkaRecordToRowConverter.kafkaSchema(includeHeaders)

  /** Returns the maximum available offset for this source. */
  override def getOffset: Option[Offset] = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    val latest = kafkaReader.fetchLatestOffsets(
      currentPartitionOffsets.orElse(Some(initialPartitionOffsets)))
    val offsets = maxOffsetsPerTrigger match {
      case None =>
        latest
      case Some(limit) if currentPartitionOffsets.isEmpty =>
        rateLimit(limit, initialPartitionOffsets, latest)
      case Some(limit) =>
        rateLimit(limit, currentPartitionOffsets.get, latest)
    }

    currentPartitionOffsets = Some(offsets)
    logDebug(s"GetOffset: ${offsets.toSeq.map(_.toString).sorted}")
    Some(KafkaSourceOffset(offsets))
  }

  /** Proportionally distribute limit number of offsets among topicpartitions */
  private def rateLimit(
      limit: Long,
      from: Map[TopicPartition, Long],
      until: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    val fromNew = kafkaReader.fetchEarliestOffsets(until.keySet.diff(from.keySet).toSeq)
    val sizes = until.flatMap {
      case (tp, end) =>
        // If begin isn't defined, something's wrong, but let alert logic in getBatch handle it
        from.get(tp).orElse(fromNew.get(tp)).flatMap { begin =>
          val size = end - begin
          logDebug(s"rateLimit $tp size is $size")
          if (size > 0) Some(tp -> size) else None
        }
    }
    val total = sizes.values.sum.toDouble
    if (total < 1) {
      until
    } else {
      until.map {
        case (tp, end) =>
          tp -> sizes.get(tp).map { size =>
            val begin = from.getOrElse(tp, fromNew(tp))
            val prorate = limit * (size / total)
            logDebug(s"rateLimit $tp prorated amount is $prorate")
            // Don't completely starve small topicpartitions
            val prorateLong = (if (prorate < 1) Math.ceil(prorate) else Math.floor(prorate)).toLong
            // need to be careful of integer overflow
            // therefore added canary checks where to see if off variable could be overflowed
            // refer to [https://issues.apache.org/jira/browse/SPARK-26718]
            val off = if (prorateLong > Long.MaxValue - begin) {
              Long.MaxValue
            } else {
              begin + prorateLong
            }
            logDebug(s"rateLimit $tp new offset is $off")
            // Paranoia, make sure not to return an offset that's past end
            Math.min(end, off)
          }.getOrElse(end)
      }
    }
  }

  /**
   * Returns the data that is between the offsets
   * [`start.get.partitionToOffsets`, `end.partitionToOffsets`), i.e. end.partitionToOffsets is
   * exclusive.
   */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    // Make sure initialPartitionOffsets is initialized
    initialPartitionOffsets

    logInfo(s"GetBatch called with start = $start, end = $end")
    val untilPartitionOffsets = KafkaSourceOffset.getPartitionOffsets(end)
    // On recovery, getBatch will get called before getOffset
    if (currentPartitionOffsets.isEmpty) {
      currentPartitionOffsets = Some(untilPartitionOffsets)
    }
    if (start.isDefined && start.get == end) {
      return sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    }
    val fromPartitionOffsets = start match {
      case Some(prevBatchEndOffset) =>
        KafkaSourceOffset.getPartitionOffsets(prevBatchEndOffset)
      case None =>
        initialPartitionOffsets
    }

    // Find the new partitions, and get their earliest offsets
    val newPartitions = untilPartitionOffsets.keySet.diff(fromPartitionOffsets.keySet)
    val newPartitionOffsets = kafkaReader.fetchEarliestOffsets(newPartitions.toSeq)
    if (newPartitionOffsets.keySet != newPartitions) {
      // We cannot get from offsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionOffsets.keySet)
      reportDataLoss(
        s"Cannot find earliest offsets of ${deletedPartitions}. Some data may have been missed")
    }
    logInfo(s"Partitions added: $newPartitionOffsets")
    newPartitionOffsets.filter(_._2 != 0).foreach { case (p, o) =>
      reportDataLoss(
        s"Added partition $p starts from $o instead of 0. Some data may have been missed")
    }

    val deletedPartitions = fromPartitionOffsets.keySet.diff(untilPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      val message = if (kafkaReader.driverKafkaParams.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
        s"$deletedPartitions are gone. ${CUSTOM_GROUP_ID_ERROR_MESSAGE}"
      } else {
        s"$deletedPartitions are gone. Some data may have been missed."
      }
      reportDataLoss(message)
    }

    // Use the until partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val topicPartitions = untilPartitionOffsets.keySet.filter { tp =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionOffsets.contains(tp) || fromPartitionOffsets.contains(tp)
    }.toSeq
    logDebug("TopicPartitions: " + topicPartitions.mkString(", "))

    val sortedExecutors = getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.length
    logDebug("Sorted executors: " + sortedExecutors.mkString(", "))

    // Calculate offset ranges
    val offsetRanges = topicPartitions.map { tp =>
      val fromOffset = fromPartitionOffsets.getOrElse(tp, newPartitionOffsets.getOrElse(tp, {
        // This should not happen since newPartitionOffsets contains all partitions not in
        // fromPartitionOffsets
        throw new IllegalStateException(s"$tp doesn't have a from offset")
      }))
      val untilOffset = untilPartitionOffsets(tp)
      val preferredLoc = if (numExecutors > 0) {
        // This allows cached KafkaConsumers in the executors to be re-used to read the same
        // partition in every batch.
        Some(sortedExecutors(Math.floorMod(tp.hashCode, numExecutors)))
      } else None
      KafkaSourceRDDOffsetRange(tp, fromOffset, untilOffset, preferredLoc)
    }.filter { range =>
      if (range.untilOffset < range.fromOffset) {
        reportDataLoss(s"Partition ${range.topicPartition}'s offset was changed from " +
          s"${range.fromOffset} to ${range.untilOffset}, some data may have been missed")
        false
      } else {
        true
      }
    }.toArray

    // Create an RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = if (includeHeaders) {
      new KafkaSourceRDD(
        sc, executorKafkaParams, offsetRanges, pollTimeoutMs, failOnDataLoss)
        .map(converter.toInternalRowWithHeaders)
    } else {
      new KafkaSourceRDD(
        sc, executorKafkaParams, offsetRanges, pollTimeoutMs, failOnDataLoss)
        .map(converter.toInternalRowWithoutHeaders)
    }

    logInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topicPartition.toString).mkString(", "))

    sqlContext.internalCreateDataFrame(rdd.setName("kafka"), schema, isStreaming = true)
  }

  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = synchronized {
    kafkaReader.close()
  }

  override def toString(): String = s"KafkaSourceV1[$kafkaReader]"

  /**
   * If `failOnDataLoss` is true, this method will throw an `IllegalStateException`.
   * Otherwise, just log a warning.
   */
  private def reportDataLoss(message: String): Unit = {
    if (failOnDataLoss) {
      throw new IllegalStateException(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_TRUE")
    } else {
      logWarning(message + s". $INSTRUCTION_FOR_FAIL_ON_DATA_LOSS_FALSE")
    }
  }
}

/** Companion object for the [[KafkaSource]]. */
private[kafka010] object KafkaSource {
  def getSortedExecutorList(sc: SparkContext): Array[String] = {
    val bm = sc.env.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }

  private def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
    if (a.host == b.host) { a.executorId > b.executorId } else { a.host > b.host }
  }

}
