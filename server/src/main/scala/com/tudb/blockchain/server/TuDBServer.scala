package com.tudb.blockchain.server

import com.tudb.blockchain.eth.{EthBlockchainSynchronizer, Web3jEthClient}
import com.tudb.storage.RocksDBStorageConfig
import com.tudb.storage.meta.MetaStoreApi
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.rocksdb.RocksDB

import java.util.concurrent.TimeUnit

/**
  *@description:
  */
class TuDBServer(context: TuDBServerContext) {
  val metaDB: RocksDB =
    RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${context.getTuDBPath()}/meta.db")
  val metaStoreApi = new MetaStoreApi(metaDB)

  val chainNames: Seq[String] = metaStoreApi.getAllBlockchainNames()

  val chainDBs: Map[String, RocksDB] = chainNames
    .map(chainName =>
      chainName -> RocksDB.open(
        RocksDBStorageConfig.getDefaultOptions(true),
        s"${context.getTuDBPath()}/${chainName}.db"
      )
    )
    .toMap

  // TODO: abstract the synchronizer to adapt all blockchain
  val ethClient = new Web3jEthClient(context.getEthNodeUrl())
  val synchronizer = new EthBlockchainSynchronizer(ethClient, chainDBs("ethereum"), metaStoreApi)

  private val server: Server =
    NettyServerBuilder
      .forPort(context.getTuDBPort())
      .addService(new TuDBQueryService(chainDBs, metaStoreApi))
      .build()

  def start(): Unit = {
    println("server start...")
    server.start()
    println("synchronizer start...")
    synchronizer.start()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        server.shutdown()
      }
    })
    server.awaitTermination()
  }

  def shutdown(): Unit = {
    server.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    synchronizer.stop()
    chainDBs.values.foreach(db => db.close())
    metaDB.close()
  }
}
