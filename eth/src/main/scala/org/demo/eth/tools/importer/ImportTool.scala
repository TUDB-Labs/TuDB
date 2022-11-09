package org.demo.eth.tools.importer

import com.alibaba.fastjson.JSONObject
import org.apache.commons.io.FileUtils
import org.demo.eth.db.EthRocksDBStorageConfig
import org.demo.eth.eth.{EthJsonApi, EthNodeClient}
import org.demo.eth.tools.JsonTools
import org.rocksdb.RocksDB

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
  *@description:
  */
class ImportTool(dbPath: String, dataSize: Int) {

  def importData() {
    val file = new File(dbPath)
    if (file.exists()) FileUtils.deleteDirectory(file)
    val db = RocksDB.open(EthRocksDBStorageConfig.getDefault(true), dbPath)

    val msgQueue: ConcurrentLinkedQueue[JSONObject] = new ConcurrentLinkedQueue[JSONObject]()
    val client = new EthNodeClient("192.168.31.178", 8546, msgQueue)
    client.connect

    try {
      println("Pull Blockchain latest BlockNumber....")
      client.sendJsonRequest(EthJsonApi.getEthBlockNumber(1))
      val countBlockNumber: AtomicInteger = new AtomicInteger(
        JsonTools.getBlockNumber(client.consumeResult())
      )
      println(s"Current Blockchain latest BlockNumber is: ${countBlockNumber.get()}")

      val importer = new PullDataFromEthNode(db, client, countBlockNumber, msgQueue)
      importer.pullTransactionFromNode(dataSize)
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
