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
package org.apache.spark.scheduler.cluster.k8s

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import scala.concurrent.Future

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.SparkContext
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.security.HadoopDelegationTokenManager
import org.apache.spark.internal.config.SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO
import org.apache.spark.rpc.RpcAddress
import org.apache.spark.scheduler.{ExecutorKilled, ExecutorLossReason, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class KubernetesClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    kubernetesClient: KubernetesClient,
    executorService: ScheduledExecutorService,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    podAllocator: ExecutorPodsAllocator,
    lifecycleEventHandler: ExecutorPodsLifecycleManager,
    watchEvents: ExecutorPodsWatchSnapshotSource,
    pollEvents: ExecutorPodsPollingSnapshotSource)
    extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

  protected override val minRegisteredRatio =
    if (conf.get(SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO).isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  private val shouldDeleteExecutors = conf.get(KUBERNETES_DELETE_EXECUTORS)

  // Allow removeExecutor to be accessible by ExecutorPodsLifecycleEventHandler
  private[k8s] def doRemoveExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    if (isExecutorActive(executorId)) {
      removeExecutor(executorId, reason)
    }
  }

  /**
   * Get an application ID associated with the job.
   * This returns the string value of spark.app.id if set, otherwise
   * the locally-generated ID from the superclass.
   *
   * @return The application ID
   */
  override def applicationId(): String = {
    conf.getOption("spark.app.id").map(_.toString).getOrElse(super.applicationId)
  }

  override def start(): Unit = {
    super.start()
    podAllocator.setTotalExpectedExecutors(initialExecutors)
    lifecycleEventHandler.start(this)
    podAllocator.start(applicationId())
    watchEvents.start(applicationId())
    pollEvents.start(applicationId())
  }

  override def stop(): Unit = {
    super.stop()

    Utils.tryLogNonFatalError {
      snapshotsStore.stop()
    }

    Utils.tryLogNonFatalError {
      watchEvents.stop()
    }

    Utils.tryLogNonFatalError {
      pollEvents.stop()
    }

    if (shouldDeleteExecutors) {
      Utils.tryLogNonFatalError {
        kubernetesClient
          .pods()
          .withLabel(SPARK_APP_ID_LABEL, applicationId())
          .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
          .delete()
      }
    }

    Utils.tryLogNonFatalError {
      ThreadUtils.shutdown(executorService)
    }

    Utils.tryLogNonFatalError {
      kubernetesClient.close()
    }
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    podAllocator.setTotalExpectedExecutors(requestedTotal)
    Future.successful(true)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def getExecutorIds(): Seq[String] = synchronized {
    super.getExecutorIds()
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = {
    executorIds.foreach { id =>
      removeExecutor(id, ExecutorKilled)
    }

    // Give some time for the executors to shut themselves down, then forcefully kill any
    // remaining ones. This intentionally ignores the configuration about whether pods
    // should be deleted; only executors that shut down gracefully (and are then collected
    // by the ExecutorPodsLifecycleManager) will respect that configuration.
    val killTask = new Runnable() {
      override def run(): Unit = Utils.tryLogNonFatalError {
        val running = kubernetesClient
          .pods()
          .withField("status.phase", "Running")
          .withLabel(SPARK_APP_ID_LABEL, applicationId())
          .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
          .withLabelIn(SPARK_EXECUTOR_ID_LABEL, executorIds: _*)

        if (!running.list().getItems().isEmpty()) {
          logInfo(s"Forcefully deleting ${running.list().getItems().size()} pods " +
            s"(out of ${executorIds.size}) that are still running after graceful shutdown period.")
          running.delete()
        }
      }
    }
    executorService.schedule(killTask, conf.get(KUBERNETES_DYN_ALLOC_KILL_GRACE_PERIOD),
      TimeUnit.MILLISECONDS)

    // Return an immediate success, since we can't confirm or deny that executors have been
    // actually shut down without waiting too long and blocking the allocation thread, which
    // waits on this future to complete, blocking further allocations / deallocations.
    //
    // This relies a lot on the guarantees of Spark's RPC system, that a message will be
    // delivered to the destination unless there's an issue with the connection, in which
    // case the executor will shut itself down (and the driver, separately, will just declare
    // it as "lost"). Coupled with the allocation manager keeping track of which executors are
    // pending release, returning "true" here means that eventually all the requested executors
    // will be removed.
    //
    // The cleanup timer above is just an optimization to make sure that stuck executors don't
    // stick around in the k8s server. Normally it should never delete any pods at all.
    Future.successful(true)
  }

  override def createDriverEndpoint(): DriverEndpoint = {
    new KubernetesDriverEndpoint()
  }

  override protected def createTokenManager(): Option[HadoopDelegationTokenManager] = {
    Some(new HadoopDelegationTokenManager(conf, sc.hadoopConfiguration, driverEndpoint))
  }

  private class KubernetesDriverEndpoint extends DriverEndpoint {

    override def onDisconnected(rpcAddress: RpcAddress): Unit = {
      // Don't do anything besides disabling the executor - allow the Kubernetes API events to
      // drive the rest of the lifecycle decisions
      // TODO what if we disconnect from a networking issue? Probably want to mark the executor
      // to be deleted eventually.
      addressToExecutorId.get(rpcAddress).foreach(disableExecutor)
    }
  }

}
