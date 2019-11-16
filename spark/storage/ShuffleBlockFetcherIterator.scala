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

package org.apache.spark.storage

import java.io.{InputStream, IOException}
import java.nio.channels.ClosedByInterruptException
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue}

import org.apache.commons.io.IOUtils

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.util.TransportConf
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.apache.spark.util.{CompletionIterator, TaskCompletionListener, Utils}

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics update
 * @param shuffleClient [[BlockStoreClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require two info: 1. the size (in bytes as a long
 *                        field) in order to throttle the memory usage; 2. the mapIndex for this
 *                        block, which indicate the index in the map stage.
 *                        Note that zero-sized blocks are already excluded, which happened in
 *                        [[org.apache.spark.MapOutputTracker.convertMapStatuses]].
 * @param streamWrapper A function to wrap the returned input stream.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
 * @param detectCorrupt whether to detect any corruption in fetched blocks.
 * @param shuffleMetrics used to report shuffle metrics.
 * @param doBatchFetch fetch continuous shuffle blocks from same executor in batch if the server
 *                     side supports.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: BlockStoreClient,
    blockManager: BlockManager,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean,
    detectCorruptUseExtraMemory: Boolean,
    shuffleMetrics: ShuffleReadMetricsReporter,
    doBatchFetch: Boolean)
  extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Total number of blocks to fetch. This should be equal to the total number of blocks
   * in [[blocksByAddress]] because we already filter out zero-sized blocks in [[blocksByAddress]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTimeNs = System.nanoTime()

  /** Local blocks to fetch, excluding zero-sized blocks. */
  private[this] val localBlocks = scala.collection.mutable.LinkedHashSet[(BlockId, Int)]()

  /** Remote blocks to fetch, excluding zero-sized blocks. */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * Queue of fetch requests which could not be issued the first time they were dequeued. These
   * requests are tried again when the fetch constraints are satisfied.
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  private[this] var reqsInFlight = 0

  /** Current number of blocks in flight per host:port */
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
   * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
   * at most once for those corrupted blocks.
   */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * A set to store the files used for shuffling remote huge blocks. Files in this set will be
   * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[DownloadFile]()

  private[this] val onCompleteCallback = new ShuffleFetchCompletionListener(this)

  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

  override def createTempFile(transportConf: TransportConf): DownloadFile = {
    // we never need to do any encryption or decryption here, regardless of configs, because that
    // is handled at another layer in the code.  When encryption is enabled, shuffle data is written
    // to disk encrypted in the first place, and sent over the network still encrypted.
    new SimpleDownloadFile(
      blockManager.diskBlockManager.createTempLocalBlock()._2, transportConf)
  }

  override def registerTempFileToClean(file: DownloadFile): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[storage] def cleanup(): Unit = {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, _, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.path())
      }
    }
  }

  private[this] def sendRequest(req: FetchRequest): Unit = {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    reqsInFlight += 1

    // so we can look up the block info of each blockID
    val infoMap = req.blocks.map {
      case FetchBlockInfo(blockId, size, mapIndex) => (blockId.toString, (size, mapIndex))
    }.toMap
    val remainingBlocks = new HashSet[String]() ++= infoMap.keys
    val blockIds = req.blocks.map(_.blockId.toString)
    val address = req.address

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.
        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            remainingBlocks -= blockId
            results.put(new SuccessFetchResult(BlockId(blockId), infoMap(blockId)._2,
              address, infoMap(blockId)._1, buf, remainingBlocks.isEmpty))
            logDebug("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace(s"Got remote block $blockId after ${Utils.getUsedTimeNs(startTimeNs)}")
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        results.put(new FailureFetchResult(BlockId(blockId), infoMap(blockId)._2, address, e))
      }
    }

    // Fetch remote shuffle blocks to disk when the request is too large. Since the shuffle data is
    // already encrypted and compressed over the wire(w.r.t. the related configs), we can just fetch
    // the data and write it to file directly.
    if (req.size > maxReqSizeShuffleToMem) {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, this)
    } else {
      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
        blockFetchingListener, null)
    }
  }

  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize
      + ", maxBlocksInFlightPerAddress: " + maxBlocksInFlightPerAddress)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]
    var localBlockBytes = 0L
    var remoteBlockBytes = 0L

    for ((address, blockInfos) <- blocksByAddress) {
      if (address.executorId == blockManager.blockManagerId.executorId) {
        blockInfos.find(_._2 <= 0) match {
          case Some((blockId, size, _)) if size < 0 =>
            throw new BlockException(blockId, "Negative block size " + size)
          case Some((blockId, size, _)) if size == 0 =>
            throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
          case None => // do nothing.
        }
        val mergedBlockInfos = mergeContinuousShuffleBlockIdsIfNeeded(
          blockInfos.map(info => FetchBlockInfo(info._1, info._2, info._3)).to[ArrayBuffer])
        localBlocks ++= mergedBlockInfos.map(info => (info.blockId, info.mapIndex))
        localBlockBytes += mergedBlockInfos.map(_.size).sum
      } else {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[FetchBlockInfo]
        while (iterator.hasNext) {
          val (blockId, size, mapIndex) = iterator.next()
          remoteBlockBytes += size
          if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          } else if (size == 0) {
            throw new BlockException(blockId, "Zero-sized blocks should be excluded.")
          } else {
            curBlocks += FetchBlockInfo(blockId, size, mapIndex)
            curRequestSize += size
          }
          if (curRequestSize >= targetRequestSize ||
              curBlocks.size >= maxBlocksInFlightPerAddress) {
            // Add this FetchRequest
            val mergedBlocks = mergeContinuousShuffleBlockIdsIfNeeded(curBlocks)
            remoteBlocks ++= mergedBlocks.map(_.blockId)
            remoteRequests += new FetchRequest(address, mergedBlocks)
            logDebug(s"Creating fetch request of $curRequestSize at $address "
              + s"with ${mergedBlocks.size} blocks")
            curBlocks = new ArrayBuffer[FetchBlockInfo]
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          val mergedBlocks = mergeContinuousShuffleBlockIdsIfNeeded(curBlocks)
          remoteBlocks ++= mergedBlocks.map(_.blockId)
          remoteRequests += new FetchRequest(address, mergedBlocks)
        }
      }
    }
    val totalBytes = localBlockBytes + remoteBlockBytes
    logInfo(s"Getting $numBlocksToFetch (${Utils.bytesToString(totalBytes)}) non-empty blocks " +
      s"including ${localBlocks.size} (${Utils.bytesToString(localBlockBytes)}) local blocks and " +
      s"${remoteBlocks.size} (${Utils.bytesToString(remoteBlockBytes)}) remote blocks")
    remoteRequests
  }

  private[this] def mergeContinuousShuffleBlockIdsIfNeeded(
      blocks: ArrayBuffer[FetchBlockInfo]): ArrayBuffer[FetchBlockInfo] = {

    def mergeFetchBlockInfo(toBeMerged: ArrayBuffer[FetchBlockInfo]): FetchBlockInfo = {
      val startBlockId = toBeMerged.head.blockId.asInstanceOf[ShuffleBlockId]
      FetchBlockInfo(
        ShuffleBlockBatchId(
          startBlockId.shuffleId,
          startBlockId.mapId,
          startBlockId.reduceId,
          toBeMerged.last.blockId.asInstanceOf[ShuffleBlockId].reduceId + 1),
        toBeMerged.map(_.size).sum,
        toBeMerged.head.mapIndex)
    }

    val result = if (doBatchFetch) {
      var curBlocks = new ArrayBuffer[FetchBlockInfo]
      val mergedBlockInfo = new ArrayBuffer[FetchBlockInfo]
      val iter = blocks.iterator

      while (iter.hasNext) {
        val info = iter.next()
        val curBlockId = info.blockId.asInstanceOf[ShuffleBlockId]
        if (curBlocks.isEmpty) {
          curBlocks += info
        } else {
          if (curBlockId.mapId != curBlocks.head.blockId.asInstanceOf[ShuffleBlockId].mapId) {
            mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
            curBlocks.clear()
          }
          curBlocks += info
        }
      }
      if (curBlocks.nonEmpty) {
        mergedBlockInfo += mergeFetchBlockInfo(curBlocks)
      }
      mergedBlockInfo
    } else {
      blocks
    }
    // update metrics
    numBlocksToFetch += result.size
    result
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   */
  private[this] def fetchLocalBlocks(): Unit = {
    logDebug(s"Start fetching local blocks: ${localBlocks.mkString(", ")}")
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val (blockId, mapIndex) = iter.next()
      try {
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, mapIndex, blockManager.blockManagerId,
          buf.size(), buf, false))
      } catch {
        // If we see an exception, stop immediately.
        case e: Exception =>
          e match {
            // ClosedByInterruptException is an excepted exception when kill task,
            // don't log the exception stack trace to avoid confusing users.
            // See: SPARK-28340
            case ce: ClosedByInterruptException =>
              logError("Error occurred while fetching local blocks, " + ce.getMessage)
            case ex: Exception => logError("Error occurred while fetching local blocks", ex)
          }
          results.put(new FailureFetchResult(blockId, mapIndex, blockManager.blockManagerId, e))
          return
      }
    }
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(onCompleteCallback)

    // Split local and remote blocks.
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo(s"Started $numFetches remote fetches in ${Utils.getUsedTimeNs(startTimeNs)}")

    // Get Local Blocks
    fetchLocalBlocks()
    logDebug(s"Got local blocks in ${Utils.getUsedTimeNs(startTimeNs)}")
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException()
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    var streamCompressedOrEncrypted: Boolean = false
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.nanoTime()
      result = results.take()
      val fetchWaitTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startFetchWait)
      shuffleMetrics.incFetchWaitTime(fetchWaitTime)

      result match {
        case r @ SuccessFetchResult(blockId, mapIndex, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
            shuffleMetrics.incRemoteBytesRead(buf.size)
            if (buf.isInstanceOf[FileSegmentManagedBuffer]) {
              shuffleMetrics.incRemoteBytesReadToDisk(buf.size)
            }
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          if (!localBlocks.contains((blockId, mapIndex))) {
            bytesInFlight -= size
          }
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }

          if (buf.size == 0) {
            // We will never legitimately receive a zero-size block. All blocks with zero records
            // have zero size and all zero-size blocks have no records (and hence should never
            // have been requested in the first place). This statement relies on behaviors of the
            // shuffle writers, which are guaranteed by the following test cases:
            //
            // - BypassMergeSortShuffleWriterSuite: "write with some empty partitions"
            // - UnsafeShuffleWriterSuite: "writeEmptyIterator"
            // - DiskBlockObjectWriterSuite: "commit() and close() without ever opening or writing"
            //
            // There is not an explicit test for SortShuffleWriter but the underlying APIs that
            // uses are shared by the UnsafeShuffleWriter (both writers use DiskBlockObjectWriter
            // which returns a zero-size from commitAndGet() in case no records were written
            // since the last call.
            val msg = s"Received a zero-size buffer for block $blockId from $address " +
              s"(expectedApproxSize = $size, isNetworkReqDone=$isNetworkReqDone)"
            throwFetchFailedException(blockId, mapIndex, address, new IOException(msg))
          }

          val in = try {
            buf.createInputStream()
          } catch {
            // The exception could only be throwed by local shuffle block
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              e match {
                case ce: ClosedByInterruptException =>
                  logError("Failed to create input stream from local block, " +
                    ce.getMessage)
                case e: IOException => logError("Failed to create input stream from local block", e)
              }
              buf.release()
              throwFetchFailedException(blockId, mapIndex, address, e)
          }
          try {
            input = streamWrapper(blockId, in)
            // If the stream is compressed or wrapped, then we optionally decompress/unwrap the
            // first maxBytesInFlight/3 bytes into memory, to check for corruption in that portion
            // of the data. But even if 'detectCorruptUseExtraMemory' configuration is off, or if
            // the corruption is later, we'll still detect the corruption later in the stream.
            streamCompressedOrEncrypted = !input.eq(in)
            if (streamCompressedOrEncrypted && detectCorruptUseExtraMemory) {
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              input = Utils.copyStreamUpTo(input, maxBytesInFlight / 3)
            }
          } catch {
            case e: IOException =>
              buf.release()
              if (buf.isInstanceOf[FileSegmentManagedBuffer]
                  || corruptedBlocks.contains(blockId)) {
                throwFetchFailedException(blockId, mapIndex, address, e)
              } else {
                logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                corruptedBlocks += blockId
                fetchRequests += FetchRequest(
                  address, Array(FetchBlockInfo(blockId, size, mapIndex)))
                result = null
              }
          } finally {
            // TODO: release the buf here to free memory earlier
            if (input == null) {
              // Close the underlying stream if there was an issue in wrapping the stream using
              // streamWrapper
              in.close()
            }
          }

        case FailureFetchResult(blockId, mapIndex, address, e) =>
          throwFetchFailedException(blockId, mapIndex, address, e)
      }

      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId,
      new BufferReleasingInputStream(
        input,
        this,
        currentResult.blockId,
        currentResult.mapIndex,
        currentResult.address,
        detectCorrupt && streamCompressedOrEncrypted))
  }

  def toCompletionIterator: Iterator[(BlockId, InputStream)] = {
    CompletionIterator[(BlockId, InputStream), this.type](this,
      onCompleteCallback.onComplete(context))
  }

  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight. If you cannot fetch from a remote host
    // immediately, defer the request until the next time it can be processed.

    // Process any outstanding deferred fetch requests if possible.
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // Process any regular fetch requests if possible.
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress, new Queue[FetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        send(remoteAddress, request)
      }
    }

    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      sendRequest(request)
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
    }

    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
    // given remote address.
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private[storage] def throwFetchFailedException(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId, mapId, mapIndex, reduceId, e)
      case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, _) =>
        throw new FetchFailedException(address, shuffleId, mapId, mapIndex, startReduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close() and
 * also detects stream corruption if streamCompressedOrEncrypted is true
 */
private class BufferReleasingInputStream(
    // This is visible for testing
    private[storage] val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator,
    private val blockId: BlockId,
    private val mapIndex: Int,
    private val address: BlockManagerId,
    private val detectCorruption: Boolean)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = {
    try {
      delegate.read()
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(blockId, mapIndex, address, e)
    }
  }

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = {
    try {
      delegate.skip(n)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(blockId, mapIndex, address, e)
    }
  }

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = {
    try {
      delegate.read(b)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(blockId, mapIndex, address, e)
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    try {
      delegate.read(b, off, len)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        iterator.throwFetchFailedException(blockId, mapIndex, address, e)
    }
  }

  override def reset(): Unit = delegate.reset()
}

/**
 * A listener to be called at the completion of the ShuffleBlockFetcherIterator
 * @param data the ShuffleBlockFetcherIterator to process
 */
private class ShuffleFetchCompletionListener(var data: ShuffleBlockFetcherIterator)
  extends TaskCompletionListener {

  override def onTaskCompletion(context: TaskContext): Unit = {
    if (data != null) {
      data.cleanup()
      // Null out the referent here to make sure we don't keep a reference to this
      // ShuffleBlockFetcherIterator, after we're done reading from it, to let it be
      // collected during GC. Otherwise we can hold metadata on block locations(blocksByAddress)
      data = null
    }
  }

  // Just an alias for onTaskCompletion to avoid confusing
  def onComplete(context: TaskContext): Unit = this.onTaskCompletion(context)
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * The block information to fetch used in FetchRequest.
   * @param blockId block id
   * @param size estimated size of the block. Note that this is NOT the exact bytes.
   *             Size of remote block is used to calculate bytesInFlight.
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage.
   */
  private[storage] case class FetchBlockInfo(
    blockId: BlockId,
    size: Long,
    mapIndex: Int)

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of the information for blocks to fetch from the same address.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[FetchBlockInfo]) {
    val size = blocks.map(_.size).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage.
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block. Note that this is NOT the exact bytes.
   *             Size of remote block is used to calculate bytesInFlight.
   * @param buf `ManagedBuffer` for the content.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param mapIndex the mapIndex for this block, which indicate the index in the map stage
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}
