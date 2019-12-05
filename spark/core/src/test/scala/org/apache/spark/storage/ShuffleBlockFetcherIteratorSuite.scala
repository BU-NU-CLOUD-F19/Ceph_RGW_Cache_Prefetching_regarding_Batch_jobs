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

import java.io._
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.Semaphore

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkFunSuite, TaskContext}
import org.apache.spark.network._
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.network.util.LimitedInputStream
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils


class ShuffleBlockFetcherIteratorSuite extends SparkFunSuite with PrivateMethodTester {

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  // Some of the tests are quite tricky because we are testing the cleanup behavior
  // in the presence of faults.

  /** Creates a mock [[BlockTransferService]] that returns data from the given map. */
  private def createMockTransfer(data: Map[BlockId, ManagedBuffer]): BlockTransferService = {
    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any())).thenAnswer(
      (invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(3).asInstanceOf[Array[String]]
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]

        for (blockId <- blocks) {
          if (data.contains(BlockId(blockId))) {
            listener.onBlockFetchSuccess(blockId, data(BlockId(blockId)))
          } else {
            listener.onBlockFetchFailure(blockId, new BlockNotFoundException(blockId))
          }
        }
      })
    transfer
  }

  // Create a mock managed buffer for testing
  def createMockManagedBuffer(size: Int = 1): ManagedBuffer = {
    val mockManagedBuffer = mock(classOf[ManagedBuffer])
    val in = mock(classOf[InputStream])
    when(in.read(any())).thenReturn(1)
    when(in.read(any(), any(), any())).thenReturn(1)
    when(mockManagedBuffer.createInputStream()).thenReturn(in)
    when(mockManagedBuffer.size()).thenReturn(size)
    mockManagedBuffer
  }

  test("successful 3 local reads + 2 remote reads") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure blockManager.getBlockData would return the blocks
    val localBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer())
    localBlocks.foreach { case (blockId, buf) =>
      doReturn(buf).when(blockManager).getBlockData(meq(blockId))
    }

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val remoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 3, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 4, 0) -> createMockManagedBuffer())

    val transfer = createMockTransfer(remoteBlocks)

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (localBmId, localBlocks.keys.map(blockId => (blockId, 1L, 0)).toSeq),
      (remoteBmId, remoteBlocks.keys.map(blockId => (blockId, 1L, 1)).toSeq)
    ).toIterator

    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => in,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true,
      false,
      metrics,
      false)

    // 3 local blocks fetched in initialization
    verify(blockManager, times(3)).getBlockData(any())

    for (i <- 0 until 5) {
      assert(iterator.hasNext, s"iterator should have 5 elements but actually has $i elements")
      val (blockId, inputStream) = iterator.next()

      // Make sure we release buffers when a wrapped input stream is closed.
      val mockBuf = localBlocks.getOrElse(blockId, remoteBlocks(blockId))
      // Note: ShuffleBlockFetcherIterator wraps input streams in a BufferReleasingInputStream
      val wrappedInputStream = inputStream.asInstanceOf[BufferReleasingInputStream]
      verify(mockBuf, times(0)).release()
      val delegateAccess = PrivateMethod[InputStream](Symbol("delegate"))

      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(0)).close()
      wrappedInputStream.close()
      verify(mockBuf, times(1)).release()
      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(1)).close()
      wrappedInputStream.close() // close should be idempotent
      verify(mockBuf, times(1)).release()
      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(1)).close()
    }

    // 3 local blocks, and 2 remote blocks
    // (but from the same block manager so one call to fetchBlocks)
    verify(blockManager, times(3)).getBlockData(any())
    verify(transfer, times(1)).fetchBlocks(any(), any(), any(), any(), any(), any())
  }

  test("fetch continuous blocks in batch successful 3 local reads + 2 remote reads") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure blockManager.getBlockData would return the merged block
    val localBlocks = Seq[BlockId](
      ShuffleBlockId(0, 0, 0),
      ShuffleBlockId(0, 0, 1),
      ShuffleBlockId(0, 0, 2))
    val mergedLocalBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockBatchId(0, 0, 0, 3) -> createMockManagedBuffer())
    mergedLocalBlocks.foreach { case (blockId, buf) =>
      doReturn(buf).when(blockManager).getBlockData(meq(blockId))
    }

    // Make sure remote blocks would return the merged block
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val remoteBlocks = Seq[BlockId](
      ShuffleBlockId(0, 3, 0),
      ShuffleBlockId(0, 3, 1))
    val mergedRemoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockBatchId(0, 3, 0, 2) -> createMockManagedBuffer())
    val transfer = createMockTransfer(mergedRemoteBlocks)

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (localBmId, localBlocks.map(blockId => (blockId, 1L, 0))),
      (remoteBmId, remoteBlocks.map(blockId => (blockId, 1L, 1)))
    ).toIterator

    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => in,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true,
      false,
      metrics,
      true)

    // 3 local blocks batch fetched in initialization
    verify(blockManager, times(1)).getBlockData(any())

    for (i <- 0 until 2) {
      assert(iterator.hasNext, s"iterator should have 2 elements but actually has $i elements")
      val (blockId, inputStream) = iterator.next()

      // Make sure we release buffers when a wrapped input stream is closed.
      val mockBuf = mergedLocalBlocks.getOrElse(blockId, mergedRemoteBlocks(blockId))
      // Note: ShuffleBlockFetcherIterator wraps input streams in a BufferReleasingInputStream
      val wrappedInputStream = inputStream.asInstanceOf[BufferReleasingInputStream]
      verify(mockBuf, times(0)).release()
      val delegateAccess = PrivateMethod[InputStream]('delegate)

      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(0)).close()
      wrappedInputStream.close()
      verify(mockBuf, times(1)).release()
      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(1)).close()
      wrappedInputStream.close() // close should be idempotent
      verify(mockBuf, times(1)).release()
      verify(wrappedInputStream.invokePrivate(delegateAccess()), times(1)).close()
    }

    // 2 remote blocks batch fetched
    // (but from the same block manager so one call to fetchBlocks)
    verify(blockManager, times(1)).getBlockData(any())
    verify(transfer, times(1)).fetchBlocks(any(), any(), any(), any(), any(), any())
  }

  test("release current unexhausted buffer in case the task completes early") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer())

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        Future {
          // Return the first two blocks, and wait till task completion before returning the 3rd one
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 1, 0).toString, blocks(ShuffleBlockId(0, 1, 0)))
          sem.acquire()
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 2, 0).toString, blocks(ShuffleBlockId(0, 2, 0)))
        }
      })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1L, 0)).toSeq)).toIterator

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => in,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true,
      false,
      taskContext.taskMetrics.createTempShuffleReadMetrics(),
      false)

    verify(blocks(ShuffleBlockId(0, 0, 0)), times(0)).release()
    iterator.next()._2.close() // close() first block's input stream
    verify(blocks(ShuffleBlockId(0, 0, 0)), times(1)).release()

    // Get the 2nd block but do not exhaust the iterator
    val subIter = iterator.next()._2

    // Complete the task; then the 2nd block buffer should be exhausted
    verify(blocks(ShuffleBlockId(0, 1, 0)), times(0)).release()
    taskContext.markTaskCompleted(None)
    verify(blocks(ShuffleBlockId(0, 1, 0)), times(1)).release()

    // The 3rd block should not be retained because the iterator is already in zombie state
    sem.release()
    verify(blocks(ShuffleBlockId(0, 2, 0)), times(0)).retain()
    verify(blocks(ShuffleBlockId(0, 2, 0)), times(0)).release()
  }

  test("fail all blocks if any of the remote request fails") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        Future {
          // Return the first block, and then fail.
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
          listener.onBlockFetchFailure(
            ShuffleBlockId(0, 1, 0).toString, new BlockNotFoundException("blah"))
          listener.onBlockFetchFailure(
            ShuffleBlockId(0, 2, 0).toString, new BlockNotFoundException("blah"))
          sem.release()
        }
      })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1L, 0)).toSeq))
      .toIterator

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => in,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true,
      false,
      taskContext.taskMetrics.createTempShuffleReadMetrics(),
      false)

    // Continue only after the mock calls onBlockFetchFailure
    sem.acquire()

    // The first block should be returned without an exception, and the last two should throw
    // FetchFailedExceptions (due to failure)
    iterator.next()
    intercept[FetchFailedException] { iterator.next() }
    intercept[FetchFailedException] { iterator.next() }
  }

  private def mockCorruptBuffer(size: Long = 1L, corruptAt: Int = 0): ManagedBuffer = {
    val corruptStream = new CorruptStream(corruptAt)
    val corruptBuffer = mock(classOf[ManagedBuffer])
    when(corruptBuffer.size()).thenReturn(size)
    when(corruptBuffer.createInputStream()).thenReturn(corruptStream)
    corruptBuffer
  }

  private class CorruptStream(corruptAt: Long = 0L) extends InputStream {
    var pos = 0
    var closed = false

    override def read(): Int = {
      if (pos >= corruptAt) {
        throw new IOException("corrupt")
      } else {
        pos += 1
        pos
      }
    }

    override def read(dest: Array[Byte], off: Int, len: Int): Int = {
      super.read(dest, off, len)
    }

    override def close(): Unit = { closed = true }
  }

  test("retry corrupt blocks") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)
    val corruptLocalBuffer = new FileSegmentManagedBuffer(null, new File("a"), 0, 100)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        Future {
          // Return the first block, and then fail.
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 1, 0).toString, mockCorruptBuffer())
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 2, 0).toString, corruptLocalBuffer)
          sem.release()
        }
      })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1L, 0)).toSeq)).toIterator

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => new LimitedInputStream(in, 100),
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true,
      true,
      taskContext.taskMetrics.createTempShuffleReadMetrics(),
      false)

    // Continue only after the mock calls onBlockFetchFailure
    sem.acquire()

    // The first block should be returned without an exception
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 0))

    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        Future {
          // Return the first block, and then fail.
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 1, 0).toString, mockCorruptBuffer())
          sem.release()
        }
      })

    // The next block is corrupt local block (the second one is corrupt and retried)
    intercept[FetchFailedException] { iterator.next() }

    sem.acquire()
    intercept[FetchFailedException] { iterator.next() }
  }

  test("big blocks are also checked for corruption") {
    val streamLength = 10000L
    val blockManager = mock(classOf[BlockManager])
    val localBlockManagerId = BlockManagerId("local-client", "local-client", 1)
    doReturn(localBlockManagerId).when(blockManager).blockManagerId

    // This stream will throw IOException when the first byte is read
    val corruptBuffer1 = mockCorruptBuffer(streamLength, 0)
    val blockManagerId1 = BlockManagerId("remote-client-1", "remote-client-1", 1)
    val shuffleBlockId1 = ShuffleBlockId(0, 1, 0)
    val blockLengths1 = Seq[Tuple3[BlockId, Long, Int]](
      (shuffleBlockId1, corruptBuffer1.size(), 1)
    )

    val streamNotCorruptTill = 8 * 1024
    // This stream will throw exception after streamNotCorruptTill bytes are read
    val corruptBuffer2 = mockCorruptBuffer(streamLength, streamNotCorruptTill)
    val blockManagerId2 = BlockManagerId("remote-client-2", "remote-client-2", 2)
    val shuffleBlockId2 = ShuffleBlockId(0, 2, 0)
    val blockLengths2 = Seq[Tuple3[BlockId, Long, Int]](
      (shuffleBlockId2, corruptBuffer2.size(), 2)
    )

    val transfer = createMockTransfer(
      Map(shuffleBlockId1 -> corruptBuffer1, shuffleBlockId2 -> corruptBuffer2))
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (blockManagerId1, blockLengths1),
      (blockManagerId2, blockLengths2)
    ).toIterator
    val taskContext = TaskContext.empty()
    val maxBytesInFlight = 3 * 1024
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => new LimitedInputStream(in, streamLength),
      maxBytesInFlight,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true,
      true,
      taskContext.taskMetrics.createTempShuffleReadMetrics(),
      false)

    // We'll get back the block which has corruption after maxBytesInFlight/3 because the other
    // block will detect corruption on first fetch, and then get added to the queue again for
    // a retry
    val (id, st) = iterator.next()
    assert(id === shuffleBlockId2)

    // The other block will throw a FetchFailedException
    intercept[FetchFailedException] {
      iterator.next()
    }

    // Following will succeed as it reads part of the stream which is not corrupt. This will read
    // maxBytesInFlight/3 bytes from the portion copied into memory, and remaining from the
    // underlying stream
    new DataInputStream(st).readFully(
      new Array[Byte](streamNotCorruptTill), 0, streamNotCorruptTill)

    // Following will fail as it reads the remaining part of the stream which is corrupt
    intercept[FetchFailedException] { st.read() }

    // Buffers are mocked and they return the original input corrupt streams
    assert(corruptBuffer1.createInputStream().asInstanceOf[CorruptStream].closed)
    assert(corruptBuffer2.createInputStream().asInstanceOf[CorruptStream].closed)
  }

  test("ensure big blocks available as a concatenated stream can be read") {
    val tmpDir = Utils.createTempDir()
    val tmpFile = new File(tmpDir, "someFile.txt")
    val os = new FileOutputStream(tmpFile)
    val buf = ByteBuffer.allocate(10000)
    for (i <- 1 to 2500) {
      buf.putInt(i)
    }
    os.write(buf.array())
    os.close()
    val managedBuffer = new FileSegmentManagedBuffer(null, tmpFile, 0, 10000)

    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId
    doReturn(managedBuffer).when(blockManager).getBlockData(ShuffleBlockId(0, 0, 0))
    val localBlockLengths = Seq[Tuple3[BlockId, Long, Int]](
      (ShuffleBlockId(0, 0, 0), 10000, 0)
    )
    val transfer = createMockTransfer(Map(ShuffleBlockId(0, 0, 0) -> managedBuffer))
    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (localBmId, localBlockLengths)
    ).toIterator

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => new LimitedInputStream(in, 10000),
      2048,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true,
      true,
      taskContext.taskMetrics.createTempShuffleReadMetrics(),
      false)
    val (id, st) = iterator.next()
    // Check that the test setup is correct -- make sure we have a concatenated stream.
    assert (st.asInstanceOf[BufferReleasingInputStream].delegate.isInstanceOf[SequenceInputStream])

    val dst = new DataInputStream(st)
    for (i <- 1 to 2500) {
      assert(i === dst.readInt())
    }
    assert(dst.read() === -1)
    dst.close()
  }

  test("retry corrupt blocks (disabled)") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 2, 0) -> createMockManagedBuffer()
    )

    // Semaphore to coordinate event sequence in two different threads.
    val sem = new Semaphore(0)

    val transfer = mock(classOf[BlockTransferService])
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        Future {
          // Return the first block, and then fail.
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, blocks(ShuffleBlockId(0, 0, 0)))
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 1, 0).toString, mockCorruptBuffer())
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 2, 0).toString, mockCorruptBuffer())
          sem.release()
        }
      })

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1L, 0)).toSeq))
      .toIterator

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress,
      (_, in) => new LimitedInputStream(in, 100),
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true,
      false,
      taskContext.taskMetrics.createTempShuffleReadMetrics(),
      false)

    // Continue only after the mock calls onBlockFetchFailure
    sem.acquire()

    // The first block should be returned without an exception
    val (id1, _) = iterator.next()
    assert(id1 === ShuffleBlockId(0, 0, 0))
    val (id2, _) = iterator.next()
    assert(id2 === ShuffleBlockId(0, 1, 0))
    val (id3, _) = iterator.next()
    assert(id3 === ShuffleBlockId(0, 2, 0))
  }

  test("Blocks should be shuffled to disk when size of the request is above the" +
    " threshold(maxReqSizeShuffleToMem).") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    val diskBlockManager = mock(classOf[DiskBlockManager])
    val tmpDir = Utils.createTempDir()
    doReturn{
      val blockId = TempLocalBlockId(UUID.randomUUID())
      (blockId, new File(tmpDir, blockId.name))
    }.when(diskBlockManager).createTempLocalBlock()
    doReturn(diskBlockManager).when(blockManager).diskBlockManager

    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val remoteBlocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer())
    val transfer = mock(classOf[BlockTransferService])
    var tempFileManager: DownloadFileManager = null
    when(transfer.fetchBlocks(any(), any(), any(), any(), any(), any()))
      .thenAnswer((invocation: InvocationOnMock) => {
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]
        tempFileManager = invocation.getArguments()(5).asInstanceOf[DownloadFileManager]
        Future {
          listener.onBlockFetchSuccess(
            ShuffleBlockId(0, 0, 0).toString, remoteBlocks(ShuffleBlockId(0, 0, 0)))
        }
      })

    def fetchShuffleBlock(
        blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])]): Unit = {
      // Set `maxBytesInFlight` and `maxReqsInFlight` to `Int.MaxValue`, so that during the
      // construction of `ShuffleBlockFetcherIterator`, all requests to fetch remote shuffle blocks
      // are issued. The `maxReqSizeShuffleToMem` is hard-coded as 200 here.
      val taskContext = TaskContext.empty()
      new ShuffleBlockFetcherIterator(
        taskContext,
        transfer,
        blockManager,
        blocksByAddress,
        (_, in) => in,
        maxBytesInFlight = Int.MaxValue,
        maxReqsInFlight = Int.MaxValue,
        maxBlocksInFlightPerAddress = Int.MaxValue,
        maxReqSizeShuffleToMem = 200,
        detectCorrupt = true,
        false,
        taskContext.taskMetrics.createTempShuffleReadMetrics(),
        false)
    }

    val blocksByAddress1 = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (remoteBmId, remoteBlocks.keys.map(blockId => (blockId, 100L, 0)).toSeq)).toIterator
    fetchShuffleBlock(blocksByAddress1)
    // `maxReqSizeShuffleToMem` is 200, which is greater than the block size 100, so don't fetch
    // shuffle block to disk.
    assert(tempFileManager == null)

    val blocksByAddress2 = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (remoteBmId, remoteBlocks.keys.map(blockId => (blockId, 300L, 0)).toSeq)).toIterator
    fetchShuffleBlock(blocksByAddress2)
    // `maxReqSizeShuffleToMem` is 200, which is smaller than the block size 300, so fetch
    // shuffle block to disk.
    assert(tempFileManager != null)
  }

  test("fail zero-size blocks") {
    val blockManager = mock(classOf[BlockManager])
    val localBmId = BlockManagerId("test-client", "test-client", 1)
    doReturn(localBmId).when(blockManager).blockManagerId

    // Make sure remote blocks would return
    val remoteBmId = BlockManagerId("test-client-1", "test-client-1", 2)
    val blocks = Map[BlockId, ManagedBuffer](
      ShuffleBlockId(0, 0, 0) -> createMockManagedBuffer(),
      ShuffleBlockId(0, 1, 0) -> createMockManagedBuffer()
    )

    val transfer = createMockTransfer(blocks.mapValues(_ => createMockManagedBuffer(0)))

    val blocksByAddress = Seq[(BlockManagerId, Seq[(BlockId, Long, Int)])](
      (remoteBmId, blocks.keys.map(blockId => (blockId, 1L, 0)).toSeq))

    val taskContext = TaskContext.empty()
    val iterator = new ShuffleBlockFetcherIterator(
      taskContext,
      transfer,
      blockManager,
      blocksByAddress.toIterator,
      (_, in) => in,
      48 * 1024 * 1024,
      Int.MaxValue,
      Int.MaxValue,
      Int.MaxValue,
      true,
      false,
      taskContext.taskMetrics.createTempShuffleReadMetrics(),
      false)

    // All blocks fetched return zero length and should trigger a receive-side error:
    val e = intercept[FetchFailedException] { iterator.next() }
    assert(e.getMessage.contains("Received a zero-size buffer"))
  }
}
