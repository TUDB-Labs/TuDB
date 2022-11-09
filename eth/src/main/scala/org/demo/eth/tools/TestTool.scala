package org.demo.eth.tools

import org.demo.eth.QueryApi
import org.demo.eth.db.EthRocksDBStorageConfig
import org.rocksdb.RocksDB

import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  *@description:
  */
class TestTool(dbPath: String) {

  def test1Hop(): Unit = {
    val db = RocksDB.open(EthRocksDBStorageConfig.getDefault(true), dbPath)
    val api = new QueryApi(db)
    val hopTotalCost = ArrayBuffer[Long]()
    var hopMaxCost: (String, Int, Long) = ("", 0, 0)
    var hopMaxHit: (String, Int, Long) = ("", 0, 0)

    val hopAddresses = Source
      .fromFile(new File("/mnt/nvme/tudb_demo/dbHome/hop1Address.txt"))
      .getLines()
      .toSeq
      .map(addr => EthTools.hexString2ArrayBytes(addr))

    hopAddresses.foreach(address => {
      val start = System.nanoTime()
      val resCount = api.hop1Query(address).length
      val cost = System.nanoTime() - start

      if (cost > hopMaxCost._3)
        hopMaxCost = (EthTools.arrayBytes2HexString(address), resCount, cost)
      if (resCount > hopMaxHit._2)
        hopMaxHit = (EthTools.arrayBytes2HexString(address), resCount, cost)
      hopTotalCost.append(cost)
    })
    printTemplate(1, hopTotalCost, hopMaxCost, hopMaxHit)

    db.close()
  }

  def test2Hop(): Unit = {
    val db = RocksDB.open(EthRocksDBStorageConfig.getDefault(true), dbPath)
    val api = new QueryApi(db)
    val hopTotalCost = ArrayBuffer[Long]()
    var hopMaxCost: (String, Int, Long) = ("", 0, 0)
    var hopMaxHit: (String, Int, Long) = ("", 0, 0)

    val hopAddresses = api.innerGetAllAddresses().slice(0, 1000)

    hopAddresses.foreach(address => {
      val start = System.nanoTime()
      val resCount = api.hop2Query(address).length
      val cost = System.nanoTime() - start

      if (cost > hopMaxCost._3)
        hopMaxCost = (EthTools.arrayBytes2HexString(address), resCount, cost)
      if (resCount > hopMaxHit._2)
        hopMaxHit = (EthTools.arrayBytes2HexString(address), resCount, cost)
      hopTotalCost.append(cost)
    })
    printTemplate(2, hopTotalCost, hopMaxCost, hopMaxHit)

    db.close()
  }

  def test3Hop(): Unit = {
    val db = RocksDB.open(EthRocksDBStorageConfig.getDefault(true), dbPath)
    val api = new QueryApi(db)
    val hopTotalCost = ArrayBuffer[Long]()
    var hopMaxCost: (String, Int, Long) = ("", 0, 0)
    var hopMaxHit: (String, Int, Long) = ("", 0, 0)

    val hopAddresses = Source
      .fromFile(new File("/mnt/nvme/tudb_demo/dbHome/hop3Address.txt"))
      .getLines()
      .toSeq
      .map(addr => EthTools.hexString2ArrayBytes(addr))

    hopAddresses.foreach(address => {
      val start = System.nanoTime()
      val resCount = api.hop3Query(address).length
      val cost = System.nanoTime() - start

      if (cost > hopMaxCost._3)
        hopMaxCost = (EthTools.arrayBytes2HexString(address), resCount, cost)
      if (resCount > hopMaxHit._2)
        hopMaxHit = (EthTools.arrayBytes2HexString(address), resCount, cost)
      hopTotalCost.append(cost)
    })
    printTemplate(3, hopTotalCost, hopMaxCost, hopMaxHit)

    db.close()
  }

  def printTemplate(
      hop: Int,
      hopTotalCost: ArrayBuffer[Long],
      hopMaxCost: (String, Int, Long),
      hopMaxCount: (String, Int, Long)
    ): Unit = {
    val dataSize = 1000.0
    val nano2milli = 1000000.0
    println(s"Hop $hop test finished....")
    println("Result: ")
    println(s"$hop hop query avg time cost: ${hopTotalCost.sum / dataSize / nano2milli} ms")
    println(s"$hop hop address of max time cost: ")
    println(
      s"Address: ${hopMaxCost._1}, count addresses: ${hopMaxCost._2}, time cost: ${hopMaxCost._3 / nano2milli} ms"
    )
    println(s"$hop hop address of max hit cost: ")
    println(
      s"Address: ${hopMaxCount._1}, count addresses: ${hopMaxCount._2}, time cost: ${hopMaxCount._3 / nano2milli} ms"
    )
    println("=======================================")
    println()
  }
  def prepareData(api: QueryApi, from: Int, to: Int): Iterator[Array[Byte]] = {
    api.innerGetAllAddresses().slice(from, to)
  }
}
