package com.tudb.blockchain.server

import com.tudb.blockchain.eth.synchronizer.{EthBlockChainSynchronizer}
import com.tudb.blockchain.storage.{QueryApi, RocksDBStorageConfig, TuMetaApi}
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.rocksdb.RocksDB

import java.util.concurrent.{TimeUnit}

/**
  *@description:
  */
class TuDBServer(context: TuDBServerContext) {
  val db = RocksDB.open(RocksDBStorageConfig.getDefault(true), context.getTuDBPath())
  val queryApi = new QueryApi(db)
  val metaApi = new TuMetaApi(db)

  val synchronizer =
    new EthBlockChainSynchronizer(db, context.getEthNodeHost(), context.getEthNodePort())

  private val server: Server =
    NettyServerBuilder
      .forPort(context.getTuDBPort())
      .addService(new TuDBQueryService(queryApi))
      .build()

  def start(): Unit = {
    println("server start...")
    server.start()
    println("synchronizer start...")
    synchronizer.synchronizeBlockChainData()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        server.shutdown()
      }
    })
    server.awaitTermination()
  }

  def shutdown(): Unit = {
    server.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    synchronizer.shutdown()
  }
}
