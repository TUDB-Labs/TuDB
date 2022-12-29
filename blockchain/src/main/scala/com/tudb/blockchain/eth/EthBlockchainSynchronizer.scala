package com.tudb.blockchain.eth

import com.tudb.blockchain.importer.BlockchainTransactionImporter
import com.tudb.storage.meta.MetaStoreApi
import org.rocksdb.RocksDB

import java.math.BigInteger
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
  *@description:
  */
class EthBlockchainSynchronizer(
    ethClient: Web3jEthClient,
    chainDB: RocksDB,
    metaStoreApi: MetaStoreApi) {
  val blockParser = new EthBlockParser()
  val importer = new BlockchainTransactionImporter(chainDB, metaStoreApi)
  var currentSynchronizedBlockNumber: Long = metaStoreApi.getSynchronizedBlockNumber("ethereum")

  private val scheduleRequestBlockNumber: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()

  def start(): Unit = {
    scheduleRequestBlockNumber.scheduleWithFixedDelay(
      new Runnable {
        override def run(): Unit = {
          val toSynchronizeBlockNumber = currentSynchronizedBlockNumber + 1
          val blockData = ethClient
            .getEthBlockInfoByNumber(new BigInteger(s"$toSynchronizeBlockNumber", 10), true)
          if (blockData != null) {
            val txs = blockParser.getBlockTransactions(blockData)
            importer.importTx(txs)
            currentSynchronizedBlockNumber += 1
            metaStoreApi.setSynchronizedBlockNumber("ethereum", currentSynchronizedBlockNumber)
            println(
              s"Synchronized BlockNumber: $toSynchronizeBlockNumber, Import: ${txs.length} transactions."
            )
          }
        }
      },
      0,
      10,
      TimeUnit.SECONDS
    )
  }

  def stop(): Unit = {
    scheduleRequestBlockNumber.shutdown()
  }
}
