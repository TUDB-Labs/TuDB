package com.tudb.blockchain.server

import com.alibaba.fastjson.JSONObject
import com.tudb.blockchain.eth.{EthBlockChainSynchronizer, EthJsonParser, EthNodeClient, EthNodeJsonApi}
import com.tudb.blockchain.eth.meta.MetaKeyManager
import com.tudb.blockchain.storage.{QueryApi, RocksDBStorageConfig}
import com.tudb.blockchain.tools.ByteUtils
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.rocksdb.RocksDB

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, Executors, TimeUnit}

/**
  *@description:
  */
class TuDBServer(dbPath: String, port: Int) {
  val db = RocksDB.open(RocksDBStorageConfig.getDefault(true), dbPath)
  val queryApi = new QueryApi(db)

  // timed task: synchronize block data
  val blockChainSynchronizer = new EthBlockChainSynchronizer(db)

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
