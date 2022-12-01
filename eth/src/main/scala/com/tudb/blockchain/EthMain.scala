package com.tudb.blockchain

import com.alibaba.fastjson.JSONObject
import com.tudb.blockchain.eth.importer.PullDataFromEthNode
import com.tudb.blockchain.eth.{EthJsonParser, EthNodeClient, EthNodeJsonApi}
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
    val queue = new ConcurrentLinkedQueue[JSONObject]()
    val client = new EthNodeClient("192.168.31.178", 8546, queue)
    client.connect

    val file = new File(dbPath)
    if (file.exists()) FileUtils.deleteDirectory(file)

    val db = RocksDB.open(RocksDBStorageConfig.getDefault(true), dbPath)

    client.sendJsonRequest(EthNodeJsonApi.getEthBlockNumber(1))
    val currentBlockNumber: AtomicInteger = new AtomicInteger(
      EthJsonParser.getBlockNumber(client.consumeResult())
    )
    val pullRunner = new PullDataFromEthNode(db, client, currentBlockNumber, queue)
    pullRunner.pullTransactionFromNode(1000)

    client.close()
    db.close()
  }

}
