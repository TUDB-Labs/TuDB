package org.demo.eth.tools.importer

import com.alibaba.fastjson.JSONObject
import org.demo.eth.eth.{EthJsonApi, EthNodeClient}
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
    5,
    TimeUnit.SECONDS
  )

  // send request
  val sendRequestService = Executors.newSingleThreadScheduledExecutor()
  sendRequestService.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        if (msgQueue.size() <= 100000 && blockNumber.get() > 0) {
          val blockNum = blockNumber.getAndDecrement()
          if (blockNum > 0)
            client.sendJsonRequest(EthJsonApi.getBlockByNumber(blockNum, true, blockNum))
        }
      }
    },
    0,
    10,
    TimeUnit.MILLISECONDS
  )

  def pullTransactionFromNode(importSize: Int): Unit = {
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
}
