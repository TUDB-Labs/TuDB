package com.tudb.blockchain.storage

import com.tudb.blockchain.tools.DataConverter
import org.rocksdb.RocksDB

/**
  *@description:
  */
class QueryApi(db: RocksDB) {
  private val innerQueryApi = new InnerQueryApi(db)

  def findOutTransaction(address: String): Iterator[(String, String)] = {
    val innerAddress =
      DataConverter.hexString2ArrayBytes(DataConverter.removeHexStringHeader(address))

    innerQueryApi
      .innerFindOutKey(innerAddress)
      .map(bytes => {
        val key = bytes._1
        val wei = bytes._2
        (
          "0x" + DataConverter.arrayBytes2HexString(key.slice(23, 43)),
          "0x" + DataConverter.arrayBytes2HexString(wei)
        )
      })
  }
  def findInTransaction(address: String): Iterator[(String, String)] = {
    val innerAddress =
      DataConverter.hexString2ArrayBytes(DataConverter.removeHexStringHeader(address))

    innerQueryApi
      .innerFindInKey(innerAddress)
      .map(bytes => {
        val key = bytes._1
        val wei = bytes._2
        (
          "0x" + DataConverter.arrayBytes2HexString(key.slice(23, 43)),
          "0x" + DataConverter.arrayBytes2HexString(wei)
        )
      })
  }
  def allAddress(): Iterator[String] = {
    innerQueryApi
      .innerGetAllAddresses()
      .map(bytes => {
        "0x" + DataConverter.arrayBytes2HexString(bytes.slice(3, 23))
      })
  }

}
