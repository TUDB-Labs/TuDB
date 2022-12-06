package com.tudb.blockchain.eth.synchronizer

import com.tudb.blockchain.eth.client.{EthClientApi}
import com.tudb.blockchain.eth.meta.MetaKeyManager
import com.tudb.blockchain.storage.TuMetaApi
import com.tudb.blockchain.tools.ByteUtils
import org.rocksdb.RocksDB

import java.util.concurrent.{Executors, TimeUnit}

/**
  *@description:
  */
class EthBlockChainSynchronizer(db: RocksDB, host: String, port: Int) {
  val clientApi = new EthClientApi("192.168.31.178", 8546)
  val metaApi = new TuMetaApi(db)
  def synchronizeFullEthBlockChainData(): Unit = {
    val currentBlockChainNumber = clientApi.getBlockChainNumber()

    var importFlag = currentBlockChainNumber
    var stopFlag = currentBlockChainNumber
    var countTransaction: Long = 0

    // transaction watcher
    val importWatcher = Executors.newSingleThreadScheduledExecutor()
    importWatcher.scheduleWithFixedDelay(
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
          if (importFlag > 0) {
            clientApi.sendPullBlockDataRequest(importFlag)
            importFlag -= 1
          }
        }
      },
      0,
      5,
      TimeUnit.MILLISECONDS
    )

    val txImporter = new TransactionImporter(db, clientApi)
    while (stopFlag > 0) {
      val isImported = txImporter.importer()
      if (isImported._1) {
        stopFlag -= 1
        countTransaction += isImported._2
      } else Thread.sleep(100)
    }

    metaApi.setSynchronizedBlockNumber(currentBlockChainNumber)

    importWatcher.shutdown()
    sendRequestService.shutdown()
  }

  def synchronizeBlock(): Unit = {
    val currentSynchronizedBlockNumber = {
      val blockNumber = db.get(MetaKeyManager.blockNumberKey)
      if (blockNumber == null) 0
      else ByteUtils.getInt(blockNumber, 0)
    }

    try {
      var count: Int = 0
      val currentBlockChainNumber: Int = clientApi.getBlockChainNumber()
      for (blockNumber <- currentSynchronizedBlockNumber + 1 to currentBlockChainNumber) {
        clientApi.sendPullBlockDataRequest(blockNumber)
        count += 1
      }

      val txImporter = new TransactionImporter(db, clientApi)
      while (count > 0) {
        val isImported = txImporter.importer()
        if (isImported._1) {
          count -= 1
        } else Thread.sleep(100)
      }
      // update meta
      metaApi.setSynchronizedBlockNumber(currentBlockChainNumber)

    } catch {
      case e: Exception => {
        println(s"client error...${e.getMessage}")
      }
    }
  }
}
