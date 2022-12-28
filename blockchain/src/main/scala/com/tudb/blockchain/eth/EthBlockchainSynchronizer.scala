package com.tudb.blockchain.eth

import org.rocksdb.RocksDB

import java.util.concurrent.{Executors, ScheduledExecutorService}

/**
  *@description:
  */
class EthBlockchainSynchronizer(ethClient: Web3jEthClient, db: RocksDB) {
  private val scheduleRequestLatestBlockNumber: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()

}
