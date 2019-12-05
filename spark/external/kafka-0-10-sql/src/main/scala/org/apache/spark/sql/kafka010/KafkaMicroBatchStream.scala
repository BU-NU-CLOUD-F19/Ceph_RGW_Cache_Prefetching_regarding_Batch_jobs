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

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Network.NETWORK_TIMEOUT
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.sources.RateControlMicroBatchStream
import org.apache.spark.sql.kafka010.KafkaSourceProvider._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.UninterruptibleThread

/**
 * A [[MicroBatchStream]] that reads data from Kafka.
 *
 * The [[KafkaSourceOffset]] is the custom [[Offset]] defined for this source that contains
 * a map of TopicPartition -> offset. Note that this offset is 1 + (available offset). For
 * example if the last record in a Kafka topic "t", partition 2 is offset 5, then
 * KafkaSourceOffset will contain TopicPartition("t", 2) -> 6. This is done keep it consistent
 * with the semantics of `KafkaConsumer.position()`.
 *
 * Zero data lost is not guaranteed when topics are deleted. If zero data lost is critical, the user
 * must make sure all messages in a topic have been processed when deleting a topic.
 *
 * There is a known issue caused by KAFKA-1894: the query using Kafka maybe cannot be stopped.
 * To avoid this issue, you should make sure stopping the query before stopping the Kafka brokers
 * and not use wrong broker addresses.
 */
private[kafka010] class KafkaMicroBatchStream(
    private[kafka010] val kafkaOffsetReader: KafkaOffsetReader,
    executorKafkaParams: ju.Map[String, Object],
    options: CaseInsensitiveStringMap,
    metadataPath: String,
    startingOffsets: KafkaOffsetRangeLimit,
    failOnDataLoss: Boolean) extends RateControlMicroBatchStream with Logging {

  private[kafka010] val pollTimeoutMs = options.getLong(
    KafkaSourceProvider.CONSUMER_POLL_TIMEOUT,
    SparkEnv.get.conf.get(NETWORK_TIMEOUT) * 1000L)

  private[kafka010] val maxOffsetsPerTrigger = Option(options.get(
    KafkaSourceProvider.MAX_OFFSET_PER_TRIGGER)).map(_.toLong)

  private val includeHeaders = options.getBoolean(INCLUDE_HEADERS, false)

  private val rangeCalculator = KafkaOffsetRangeCalculator(options)

  private var endPartitionOffsets: KafkaSourceOffset = _

  /**
   * Lazily initialize `initialPartitionOffsets` to make sure that `KafkaConsumer.poll` is only
   * called in StreamExecutionThread. Otherwise, interrupting a thread while running
   * `KafkaConsumer.poll` may hang forever (KAFKA-1894).
   */
  override def initialOffset(): Offset = {
    KafkaSourceOffset(getOrCreateInitialPartitionOffsets())
  }

  override def latestOffset(start: Offset): Offset = {
    val startPartitionOffsets = start.asInstanceOf[KafkaSourceOffset].partitionToOffsets
    val latestPartitionOffsets = kafkaOffsetReader.fetchLatestOffsets(Some(startPartitionOffsets))
    endPartitionOffsets = KafkaSourceOffset(maxOffsetsPerTrigger.map { maxOffsets =>
      rateLimit(maxOffsets, startPartitionOffsets, latestPartitionOffsets)
    }.getOrElse {
      latestPartitionOffsets
    })
    endPartitionOffsets
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startPartitionOffsets = start.asInstanceOf[KafkaSourceOffset].partitionToOffsets
    val endPartitionOffsets = end.asInstanceOf[KafkaSourceOffset].partitionToOffsets

    // Find the new partitions, and get their earliest offsets
    val newPartitions = endPartitionOffsets.keySet.diff(startPartitionOffsets.keySet)
    val newPartitionInitialOffsets = kafkaOffsetReader.fetchEarliestOffsets(newPartitions.toSeq)
    if (newPartitionInitialOffsets.keySet != newPartitions) {
      // We cannot get from offsets for some partitions. It means they got deleted.
      val deletedPartitions = newPartitions.diff(newPartitionInitialOffsets.keySet)
      reportDataLoss(
        s"Cannot find earliest offsets of ${deletedPartitions}. Some data may have been missed")
    }
    logInfo(s"Partitions added: $newPartitionInitialOffsets")
    newPartitionInitialOffsets.filter(_._2 != 0).foreach { case (p, o) =>
      reportDataLoss(
        s"Added partition $p starts from $o instead of 0. Some data may have been missed")
    }

    // Find deleted partitions, and report data loss if required
    val deletedPartitions = startPartitionOffsets.keySet.diff(endPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      val message =
        if (kafkaOffsetReader.driverKafkaParams.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
          s"$deletedPartitions are gone. ${CUSTOM_GROUP_ID_ERROR_MESSAGE}"
        } else {
          s"$deletedPartitions are gone. Some data may have been missed."
        }
      reportDataLoss(message)
    }

    // Use the end partitions to calculate offset ranges to ignore partitions that have
    // been deleted
    val topicPartitions = endPartitionOffsets.keySet.filter { tp =>
      // Ignore partitions that we don't know the from offsets.
      newPartitionInitialOffsets.contains(tp) || startPartitionOffsets.contains(tp)
    }.toSeq
    logDebug("TopicPartitions: " + topicPartitions.mkString(", "))

    val fromOffsets = startPartitionOffsets ++ newPartitionInitialOffsets
    val untilOffsets = endPartitionOffsets
    untilOffsets.foreach { case (tp, untilOffset) =>
      fromOffsets.get(tp).foreach { fromOffset =>
        if (untilOffset < fromOffset) {
          reportDataLoss(s"Partition $tp's offset was changed from " +
            s"$fromOffset to $untilOffset, some data may have been missed")
        }
      }
    }

    // Calculate offset ranges
    val offsetRanges = rangeCalculator.getRanges(
      fromOffsets = fromOffsets,
      untilOffsets = untilOffsets,
      executorLocations = getSortedExecutorList())

    // Generate factories based on the offset ranges
    offsetRanges.map { range =>
      KafkaBatchInputPartition(range, executorKafkaParams, pollTimeoutMs,
        failOnDataLoss, includeHeaders)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    KafkaBatchReaderFactory
  }

  override def deserializeOffset(json: String): Offset = {
    KafkaSourceOffset(JsonUtils.partitionOffsets(json))
  }

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {
    kafkaOffsetReader.close()
  }

  override def toString(): String = s"KafkaV2[$kafkaOffsetReader]"

  /**
   * Read initial partition offsets from the checkpoint, or decide the offsets and write them to
   * the checkpoint.
   */
  private def getOrCreateInitialPartitionOffsets(): PartitionOffsetMap = {
    // Make sure that `KafkaConsumer.poll` is only called in StreamExecutionThread.
    // Otherwise, interrupting a thread while running `KafkaConsumer.poll` may hang forever
    // (KAFKA-1894).
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    // SparkSession is required for getting Hadoop configuration for writing to checkpoints
    assert(SparkSession.getActiveSession.nonEmpty)

    val metadataLog =
      new KafkaSourceInitialOffsetWriter(SparkSession.getActiveSession.get, metadataPath)
    metadataLog.get(0).getOrElse {
      val offsets = startingOffsets match {
        case EarliestOffsetRangeLimit =>
          KafkaSourceOffset(kafkaOffsetReader.fetchEarliestOffsets())
        case LatestOffsetRangeLimit =>
          KafkaSourceOffset(kafkaOffsetReader.fetchLatestOffsets(None))
        case SpecificOffsetRangeLimit(p) =>
          kafkaOffsetReader.fetchSpecificOffsets(p, reportDataLoss)
        case SpecificTimestampRangeLimit(p) =>
          kafkaOffsetReader.fetchSpecificTimestampBasedOffsets(p, failsOnNoMatchingOffset = true)
      }
      metadataLog.add(0, offsets)
      logInfo(s"Initial offsets: $offsets")
      offsets
    }.partitionToOffsets
  }

  /** Proportionally distribute limit number of offsets among topicpartitions */
  private def rateLimit(
      limit: Long,
      from: PartitionOffsetMap,
      until: PartitionOffsetMap): PartitionOffsetMap = {
    val fromNew = kafkaOffsetReader.fetchEarliestOffsets(until.keySet.diff(from.keySet).toSeq)
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
            // Paranoia, make sure not to return an offset that's past end
            Math.min(end, off)
          }.getOrElse(end)
      }
    }
  }

  private def getSortedExecutorList(): Array[String] = {

    def compare(a: ExecutorCacheTaskLocation, b: ExecutorCacheTaskLocation): Boolean = {
      if (a.host == b.host) {
        a.executorId > b.executorId
      } else {
        a.host > b.host
      }
    }

    val bm = SparkEnv.get.blockManager
    bm.master.getPeers(bm.blockManagerId).toArray
      .map(x => ExecutorCacheTaskLocation(x.host, x.executorId))
      .sortWith(compare)
      .map(_.toString)
  }

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
