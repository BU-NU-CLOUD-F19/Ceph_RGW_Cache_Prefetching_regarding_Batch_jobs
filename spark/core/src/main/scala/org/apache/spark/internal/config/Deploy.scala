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

package org.apache.spark.internal.config

private[spark] object Deploy {
  val RECOVERY_MODE = ConfigBuilder("spark.deploy.recoveryMode")
    .stringConf
    .createWithDefault("NONE")

  val RECOVERY_MODE_FACTORY = ConfigBuilder("spark.deploy.recoveryMode.factory")
    .stringConf
    .createWithDefault("")

  val RECOVERY_DIRECTORY = ConfigBuilder("spark.deploy.recoveryDirectory")
    .stringConf
    .createWithDefault("")

  val ZOOKEEPER_URL = ConfigBuilder("spark.deploy.zookeeper.url")
    .doc(s"When `${RECOVERY_MODE.key}` is set to ZOOKEEPER, this " +
      "configuration is used to set the zookeeper URL to connect to.")
    .stringConf
    .createOptional

  val ZOOKEEPER_DIRECTORY = ConfigBuilder("spark.deploy.zookeeper.dir")
    .stringConf
    .createOptional

  val RETAINED_APPLICATIONS = ConfigBuilder("spark.deploy.retainedApplications")
    .intConf
    .createWithDefault(200)

  val RETAINED_DRIVERS = ConfigBuilder("spark.deploy.retainedDrivers")
    .intConf
    .createWithDefault(200)

  val REAPER_ITERATIONS = ConfigBuilder("spark.dead.worker.persistence")
    .intConf
    .createWithDefault(15)

  val MAX_EXECUTOR_RETRIES = ConfigBuilder("spark.deploy.maxExecutorRetries")
    .intConf
    .createWithDefault(10)

  val SPREAD_OUT_APPS = ConfigBuilder("spark.deploy.spreadOut")
    .booleanConf
    .createWithDefault(true)

  val DEFAULT_CORES = ConfigBuilder("spark.deploy.defaultCores")
    .intConf
    .createWithDefault(Int.MaxValue)


}
