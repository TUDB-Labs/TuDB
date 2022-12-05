package com.tudb.blockchain.eth.importer

import com.alibaba.fastjson.JSONObject
import com.tudb.blockchain.eth.{EthJsonObjectParser, EthNodeClient, EthNodeJsonApi}
import com.tudb.blockchain.storage.RocksDBStorageConfig
import org.apache.commons.io.FileUtils
import org.rocksdb.RocksDB

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
  *@description:
  */
class ImportTool(dbPath: String) {

  def importFullData(): Unit = {
    val file = new File(dbPath)
    if (file.exists()) FileUtils.deleteDirectory(file)
    val db = RocksDB.open(RocksDBStorageConfig.getDefault(true), dbPath)

    val msgQueue: ConcurrentLinkedQueue[JSONObject] = new ConcurrentLinkedQueue[JSONObject]()
    val client = new EthNodeClient("192.168.31.178", 8546, msgQueue)
    client.connect

    try {
      println("Pull Blockchain latest BlockNumber....")
      client.sendJsonRequest(EthNodeJsonApi.getEthBlockNumber(1))
      val countBlockNumber: AtomicInteger = new AtomicInteger(
        EthJsonObjectParser.getBlockNumber(client.consumeResult())
      )
      println(s"Current Blockchain latest BlockNumber is: ${countBlockNumber.get()}")

      val importer = new PullDataFromEthNode(db, client, countBlockNumber, msgQueue)
      importer.pullFullTransactionFromNode()
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    } finally {
      client.close()
      db.close()
    }
  }

  def importLimitData(dataSize: Int) {
    val file = new File(dbPath)
    if (file.exists()) FileUtils.deleteDirectory(file)
    val db = RocksDB.open(RocksDBStorageConfig.getDefault(true), dbPath)

    val msgQueue: ConcurrentLinkedQueue[JSONObject] = new ConcurrentLinkedQueue[JSONObject]()
    val client = new EthNodeClient("192.168.31.178", 8546, msgQueue)
    client.connect

    try {
      println("Pull Blockchain latest BlockNumber....")
      client.sendJsonRequest(EthNodeJsonApi.getEthBlockNumber(1))
      val countBlockNumber: AtomicInteger = new AtomicInteger(
        EthJsonObjectParser.getBlockNumber(client.consumeResult())
      )
      println(s"Current Blockchain latest BlockNumber is: ${countBlockNumber.get()}")

      val importer = new PullDataFromEthNode(db, client, countBlockNumber, msgQueue)
      importer.pullLimitedTransactionFromNode(dataSize)
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    } finally {
      client.close()
      db.close()
    }
  }
}
