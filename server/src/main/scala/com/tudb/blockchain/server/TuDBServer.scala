package com.tudb.blockchain.server

import com.tudb.blockchain.eth.synchronizer.EthBlockChainSynchronizer
import com.tudb.blockchain.storage.{QueryApi, RocksDBStorageConfig}
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.rocksdb.RocksDB

import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}

/**
  *@description:
  */
class TuDBServer(dbPath: String, port: Int) {
  val db = RocksDB.open(RocksDBStorageConfig.getDefault(true), dbPath)
  val queryApi = new QueryApi(db)

  val blockChainSynchronizer = new EthBlockChainSynchronizer(db, "192.168.31.178", 8546)

  // timed task: synchronize block data
  val blockChainWatcher = Executors.newSingleThreadScheduledExecutor()
  blockChainWatcher.scheduleWithFixedDelay(
    new Runnable {
      override def run(): Unit = {
        blockChainSynchronizer.synchronizeBlock()
      }
    },
    0,
    20,
    TimeUnit.MILLISECONDS
  )

  private val _server: Server =
    NettyServerBuilder.forPort(port).addService(new TuDBQueryService(queryApi)).build()

  def start(): Unit = {
    _server.start()
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        _server.shutdown()
      }
    })
    _server.awaitTermination()
  }

  def shutdown(): Unit = {
    _server.shutdown().awaitTermination(5, TimeUnit.SECONDS)
  }
}
