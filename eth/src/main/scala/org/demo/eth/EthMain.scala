package org.demo.eth

import org.demo.eth.db.EthRocksDBStorageConfig
import org.demo.eth.tools.importer.ImportTool
import org.demo.eth.tools.EthTools
import org.rocksdb.RocksDB

import java.io.{BufferedWriter, File, FileWriter}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  *@description:
  */
object EthMain {
  def main(args: Array[String]): Unit = {
    val dbPath = args(0)
    val mode = args(1)

    mode match {
      case "import" => {
        val dataSize = args(2).toInt
        val importer = new ImportTool(dbPath, dataSize)
        importer.importData()
      }
      case "prepareAddress" => {
        val db = RocksDB.open(EthRocksDBStorageConfig.getDefault(true), dbPath)
        val api = new QueryApi(db)
        val addressesSmall = ArrayBuffer[(String, Int)]()

        val iter = api.innerGetAllAddresses()
        while (addressesSmall.length < 100) {
          val address = iter.next()
          val count = api.innerFindOutAddressesOfTransactions(address).length
          if (count < 100 && count > 1) {
            addressesSmall.append((EthTools.arrayBytes2HexString(address), count))
          }
        }

        val fileWriter = new BufferedWriter(
          new FileWriter(new File("/mnt/nvme/tudb_demo/dbHome/addressSmallToTest.txt"))
        )
        addressesSmall
          .sortBy(f => f._2)
          .foreach(a => {
            fileWriter.write(s"${a._1}-${a._2}")
            fileWriter.newLine()
          })
        fileWriter.flush()
        fileWriter.close()

        db.close()
      }
      case "single" => {
        val inAddress = args(2)
        val db = RocksDB.open(EthRocksDBStorageConfig.getDefault(true), dbPath)
        val api = new QueryApi(db)
        val address = EthTools.hexString2ArrayBytes(inAddress)

        val start = System.nanoTime()

        val hop1Res = api.innerFindOutAddressesOfTransactions(address).toSeq
        println(s"total hop1 count: ${hop1Res.length}")

        val res = hop1Res.flatMap(hop1Address => {
          api.innerFindOutAddressesOfTransactions(hop1Address).toSeq
        })
        println(res.length)
        val cost = System.nanoTime() - start
        println(s"total hop2 count: ${res.length}, total cost: ${cost} nano")

        val res2 = res.flatMap(hop2Address => {
          api.innerFindOutAddressesOfTransactions(hop2Address).toSeq
        })
        println(res2.length)
        val cost2 = System.nanoTime() - start
        println(s"total hop3 count: ${res2.length}, total cost: ${cost2} nano")

        db.close()
      }
      case "test100" => {
        val db = RocksDB.open(EthRocksDBStorageConfig.getDefault(true), dbPath)
        val api = new QueryApi(db)
        val dataSmall = Source
          .fromFile("/mnt/nvme/tudb_demo/dbHome/addressSmallToTest.txt")
          .getLines()
          .toSeq
          .map(f => f.split("-").head)
          .map(s => EthTools.hexString2ArrayBytes(s))

        val fileWriterSmall = new BufferedWriter(
          new FileWriter(new File("/mnt/nvme/tudb_demo/dbHome/smallResult.txt"))
        )
        println("start test")
        dataSmall.foreach(address => {
          val addressStr = EthTools.arrayBytes2HexString(address)
          val startTime = System.nanoTime()
          val hop1 = api.innerFindOutAddressesOfTransactions(address).toSeq
          println(hop1.length)
          val cost1 = System.nanoTime() - startTime

          val hop2 = hop1.flatMap(a1 => api.innerFindOutAddressesOfTransactions(a1)).toSeq
          println(hop2.length)
          val cost2 = System.nanoTime() - startTime

          val hop3 = hop2.flatMap(a1 => api.innerFindOutAddressesOfTransactions(a1)).toSeq
          println(hop3.length)
          val cost3 = System.nanoTime() - startTime

          val str =
            s"$addressStr-${cost1}nano-${hop1.length}-${cost2}nano-${hop2.length}-${cost3}nano-${hop3.length}"
          println(str)
          fileWriterSmall.write(str)
          fileWriterSmall.newLine()
        })
        fileWriterSmall.flush()
        fileWriterSmall.close()

        db.close()
      }
    }
  }
}
