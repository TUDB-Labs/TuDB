import com.alibaba.fastjson.JSONObject
import org.apache.commons.io.FileUtils
import org.demo.eth.QueryApi
import org.demo.eth.db.EthRocksDBStorageConfig
import org.demo.eth.eth.{EthJsonApi, EthNodeClient}
import org.demo.eth.tools.{EthTools, JsonTools}
import org.demo.eth.tools.importer.PullDataFromEthNode
import org.rocksdb.RocksDB

import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
  * @description:
  */
object DemoRunner {
  def main(args: Array[String]): Unit = {
    val dbPath = "/Users/gaolianxin/Desktop/coding/tudb/TuDB/eth/testDB/test.db"
    importer(dbPath)
    queryOutTransaction(dbPath)
  }

  def queryOutTransaction(dbPath: String): Unit = {
    val db = RocksDB.open(EthRocksDBStorageConfig.getDefault(true), dbPath)
    val api = new QueryApi(db)
    val fromAddress = api.innerGetAllAddresses()

    fromAddress.foreach(from => {
      val res = api
        .innerFindOutAddressesOfTransactions(from)
        .map(address => EthTools.arrayBytes2HexString(address))
      println(s"${EthTools.arrayBytes2HexString(from)} send money to following addresses:  ")
      res.foreach(println)
      println("==========================================================")
      println()
    })
  }

  def importer(dbPath: String): Unit = {
    val queue = new ConcurrentLinkedQueue[JSONObject]()
    val client = new EthNodeClient("192.168.31.178", 8546, queue)
    client.connect

    val file = new File(dbPath)
    if (file.exists()) FileUtils.deleteDirectory(file)

    val db = RocksDB.open(EthRocksDBStorageConfig.getDefault(true), dbPath)

    client.sendJsonRequest(EthJsonApi.getEthBlockNumber(1))
    val currentBlockNumber: AtomicInteger = new AtomicInteger(
      JsonTools.getBlockNumber(client.consumeResult())
    )
    val pullRunner = new PullDataFromEthNode(db, client, currentBlockNumber, queue)
    pullRunner.pullTransactionFromNode(10000)

    client.close()
    db.close()
  }
}
