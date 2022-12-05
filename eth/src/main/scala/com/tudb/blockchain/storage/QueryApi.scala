package com.tudb.blockchain.storage

import com.tudb.blockchain.tools.DataConverter
import org.rocksdb.RocksDB

/**
  *@description:
  */
class QueryApi(db: RocksDB) {
  private val innerQueryApi = new InnerQueryApi(db)

  def findOutAddress(address: String): Iterator[String] = {
    val innerAddress =
      DataConverter.hexString2ArrayBytes(DataConverter.removeHexStringHeader(address))

    innerQueryApi
      .innerFindOutKey(innerAddress)
      .map(bytes => {
        "0x" + DataConverter.arrayBytes2HexString(bytes.slice(23, 43))
      })
  }
  def findInAddress(address: String): Iterator[String] = {
    val innerAddress =
      DataConverter.hexString2ArrayBytes(DataConverter.removeHexStringHeader(address))

    innerQueryApi
      .innerFindInKey(innerAddress)
      .map(bytes => {
        "0x" + DataConverter.arrayBytes2HexString(bytes.slice(23, 43))
      })
  }
}
