package com.tudb.blockchain.eth.synchronizer

import com.tudb.blockchain.eth.client.EthClientApi
import com.tudb.blockchain.eth.meta.MetaKeyManager
import com.tudb.blockchain.storage.TuMetaApi
import org.rocksdb.RocksDB

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
  *@description:
  */
class EthBlockChainSynchronizer(db: RocksDB, host: String, port: Int) {
  //"192.168.31.178", 8546
  private val metaApi = new TuMetaApi(db)
  private val synchronizer: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  private var synchronizedBlockChainNumber = metaApi.getSynchronizedBlockNumber()
  val txImporter = new TransactionImporter(db)
  def synchronizeBlockChainData(): Unit = {
    synchronizer.scheduleAtFixedRate(
      new Runnable {
        val pullDataClient = new EthClientApi(host, port)
        val latestBlockWatcher = new EthClientApi(host, port)
        var latestBlockChainNumber: Int = latestBlockWatcher.getLatestBlockChainNumber(0)
        var sentBlockChainNumber: Int = synchronizedBlockChainNumber + 1
        override def run(): Unit = {
          if (sentBlockChainNumber != latestBlockChainNumber) {
            pullDataClient.sendPullBlockDataRequest(sentBlockChainNumber)
            println(s"sent block chain number: $sentBlockChainNumber")
            sentBlockChainNumber += 1
          }
          val msg = pullDataClient.consumeMessage()
          val result = pullDataClient.getBlockTransactions(msg)
          if (msg != null) {
            txImporter.importer(result)
            synchronizedBlockChainNumber += 1
            println(
              s"consume ${result.length}, synchronized block size: $synchronizedBlockChainNumber"
            )
            metaApi.setSynchronizedBlockNumber(synchronizedBlockChainNumber)
          }
          latestBlockChainNumber = latestBlockWatcher.getLatestBlockChainNumber(0)
          println(s"latest blockchain number: $latestBlockChainNumber")
        }
      },
      0,
      5,
      TimeUnit.SECONDS
    )
  }
  def shutdown(): Unit = {
    synchronizer.shutdown()
  }
}
