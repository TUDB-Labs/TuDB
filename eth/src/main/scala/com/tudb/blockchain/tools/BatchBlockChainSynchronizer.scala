package com.tudb.blockchain.tools

import com.tudb.blockchain.eth.client.EthClientApi
import com.tudb.blockchain.eth.synchronizer.TransactionImporter
import com.tudb.blockchain.storage.TuMetaApi
import org.rocksdb.RocksDB

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
  *@description:
  */
class BatchBlockChainSynchronizer(db: RocksDB, hosts: Seq[(String, Int)]) {
  private val metaApi = new TuMetaApi(db)
  private val scheduleSendRequest: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()

  private val scheduleConsumeResult: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()

  private val scheduleWatchBlockChain: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()

  private var startBlockNumber: Int = metaApi.getSynchronizedBlockNumber() + 1

  private val txImporter = new TransactionImporter(db)
  private val clients = hosts.map(kv => new EthClientApi(kv._1, kv._2))

  scheduleSendRequest.scheduleAtFixedRate(
    new Runnable {
      override def run(): Unit = {
        clients.foreach(client => {
          client.sendPullBlockDataRequest(startBlockNumber)
          startBlockNumber += 1
        })
      }
    },
    10,
    10,
    TimeUnit.MILLISECONDS
  )

}
