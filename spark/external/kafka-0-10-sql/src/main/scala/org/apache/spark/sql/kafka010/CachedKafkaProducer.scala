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
import java.util.concurrent.{ConcurrentMap, ExecutionException, TimeUnit}

import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.kafka.clients.producer.KafkaProducer
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.{KafkaConfigUpdater, KafkaRedactionUtil}

private[kafka010] object CachedKafkaProducer extends Logging {

  private type Producer = KafkaProducer[Array[Byte], Array[Byte]]

  private val defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(10)

  private lazy val cacheExpireTimeout: Long = Option(SparkEnv.get)
    .map(_.conf.get(PRODUCER_CACHE_TIMEOUT))
    .getOrElse(defaultCacheExpireTimeout)

  private val cacheLoader = new CacheLoader[Seq[(String, Object)], Producer] {
    override def load(config: Seq[(String, Object)]): Producer = {
      createKafkaProducer(config)
    }
  }

  private val removalListener = new RemovalListener[Seq[(String, Object)], Producer]() {
    override def onRemoval(
        notification: RemovalNotification[Seq[(String, Object)], Producer]): Unit = {
      val paramsSeq: Seq[(String, Object)] = notification.getKey
      val producer: Producer = notification.getValue
      if (log.isDebugEnabled()) {
        val redactedParamsSeq = KafkaRedactionUtil.redactParams(paramsSeq)
        logDebug(s"Evicting kafka producer $producer params: $redactedParamsSeq, " +
          s"due to ${notification.getCause}")
      }
      close(paramsSeq, producer)
    }
  }

  private lazy val guavaCache: LoadingCache[Seq[(String, Object)], Producer] =
    CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[Seq[(String, Object)], Producer](cacheLoader)

  private def createKafkaProducer(paramsSeq: Seq[(String, Object)]): Producer = {
    val kafkaProducer: Producer = new Producer(paramsSeq.toMap.asJava)
    if (log.isDebugEnabled()) {
      val redactedParamsSeq = KafkaRedactionUtil.redactParams(paramsSeq)
      logDebug(s"Created a new instance of KafkaProducer for $redactedParamsSeq.")
    }
    kafkaProducer
  }

  /**
   * Get a cached KafkaProducer for a given configuration. If matching KafkaProducer doesn't
   * exist, a new KafkaProducer will be created. KafkaProducer is thread safe, it is best to keep
   * one instance per specified kafkaParams.
   */
  private[kafka010] def getOrCreate(kafkaParams: ju.Map[String, Object]): Producer = {
    val updatedKafkaProducerConfiguration =
      KafkaConfigUpdater("executor", kafkaParams.asScala.toMap)
        .setAuthenticationConfigIfNeeded()
        .build()
    val paramsSeq: Seq[(String, Object)] = paramsToSeq(updatedKafkaProducerConfiguration)
    try {
      guavaCache.get(paramsSeq)
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null =>
        throw e.getCause
    }
  }

  private def paramsToSeq(kafkaParams: ju.Map[String, Object]): Seq[(String, Object)] = {
    val paramsSeq: Seq[(String, Object)] = kafkaParams.asScala.toSeq.sortBy(x => x._1)
    paramsSeq
  }

  /** For explicitly closing kafka producer */
  private[kafka010] def close(kafkaParams: ju.Map[String, Object]): Unit = {
    val paramsSeq = paramsToSeq(kafkaParams)
    guavaCache.invalidate(paramsSeq)
  }

  /** Auto close on cache evict */
  private def close(paramsSeq: Seq[(String, Object)], producer: Producer): Unit = {
    try {
      if (log.isInfoEnabled()) {
        val redactedParamsSeq = KafkaRedactionUtil.redactParams(paramsSeq)
        logInfo(s"Closing the KafkaProducer with params: ${redactedParamsSeq.mkString("\n")}.")
      }
      producer.close()
    } catch {
      case NonFatal(e) => logWarning("Error while closing kafka producer.", e)
    }
  }

  private[kafka010] def clear(): Unit = {
    logInfo("Cleaning up guava cache.")
    guavaCache.invalidateAll()
  }

  // Intended for testing purpose only.
  private def getAsMap: ConcurrentMap[Seq[(String, Object)], Producer] = guavaCache.asMap()
}
