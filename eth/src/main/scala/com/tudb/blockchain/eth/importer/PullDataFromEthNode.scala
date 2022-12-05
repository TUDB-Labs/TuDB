package com.tudb.blockchain.eth.importer

import com.alibaba.fastjson.JSONObject
import com.tudb.blockchain.eth.meta.MetaKeyManager
import com.tudb.blockchain.eth.{EthNodeClient, EthNodeJsonApi}
import com.tudb.blockchain.tools.ByteUtils
import org.rocksdb.RocksDB

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}

/**
  *@description:
  */
class PullDataFromEthNode(
    db: RocksDB,
    client: EthNodeClient,
    blockNumber: AtomicInteger,
    msgQueue: ConcurrentLinkedQueue[JSONObject]) {

  val END_BLOCK = blockNumber.get()
  val START_TIME = System.currentTimeMillis()
  println(s"start time: $START_TIME, delay: 5")

  // count imported transaction
  val countTransaction: AtomicLong = new AtomicLong(0)

  // transaction watcher
  val service = Executors.newSingleThreadScheduledExecutor()
  service.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        println(s"Imported $countTransaction transaction...")
      }
    },
    0,
    30,
    TimeUnit.SECONDS
  )

  // send request
  val sendRequestService = Executors.newSingleThreadScheduledExecutor()
  sendRequestService.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        if (msgQueue.size() <= 1000000 && blockNumber.get() > 0) {
          val blockNum = blockNumber.getAndDecrement()
          if (blockNum > 0)
            client.sendJsonRequest(EthNodeJsonApi.getBlockByNumber(blockNum, true, blockNum))
        }
      }
    },
    0,
    5,
    TimeUnit.MILLISECONDS
  )

  def pullLimitedTransactionFromNode(importSize: Int): Unit = {
    val importer = new TransactionImporter(db, msgQueue, countTransaction)
    while (countTransaction.get() <= importSize) {
      if (msgQueue.size() != 0) {
        importer.importer()
      }
    }
    println(s"Total import transaction: $countTransaction")

    service.shutdown()
    sendRequestService.shutdown()
    client.close()
  }
  def pullFullTransactionFromNode(): Unit = {
    val importer = new TransactionImporter(db, msgQueue, countTransaction)
    while (blockNumber.get() > 0 || msgQueue.size() != 0) {
      if (msgQueue.size() != 0) importer.importer()
      else Thread.sleep(100)
    }
    val importCostTime = System.currentTimeMillis() - START_TIME
    println(s"Total import transaction: $countTransaction")
    println(s"import block from 1 to ${END_BLOCK}, cost time: ${importCostTime / 1000.0} s  ")

    val blockNumberByteArray = new Array[Byte](4)
    ByteUtils.setInt(blockNumberByteArray, 0, END_BLOCK)
    db.put(MetaKeyManager.blockNumberKey, blockNumberByteArray)

    service.shutdown()
    sendRequestService.shutdown()
  }
}
