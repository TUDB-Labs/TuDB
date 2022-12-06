package com.tudb.blockchain

import com.alibaba.fastjson.JSONObject
import com.tudb.blockchain.eth.client.{EthClientApi, EthJsonObjectParser, EthNodeClient, EthNodeJsonApi}
import com.tudb.blockchain.eth.synchronizer.EthBlockChainSynchronizer
import com.tudb.blockchain.storage.RocksDBStorageConfig
import org.apache.commons.io.FileUtils
import org.rocksdb.RocksDB

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
  *@description:
  */
object EthMain {
  def main(args: Array[String]): Unit = {
    val dbPath = "./testdata/test.db"
    val file = new File(dbPath)
    if (!file.exists()) file.mkdirs()
    importer(dbPath)
  }

  def importer(dbPath: String): Unit = {
    val file = new File(dbPath)
    if (file.exists()) FileUtils.deleteDirectory(file)

    val db = RocksDB.open(RocksDBStorageConfig.getDefault(true), dbPath)
    val synchronize = new EthBlockChainSynchronizer(db, "192.168.31.178", 8546)

  }

}
