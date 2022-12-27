package com.tudb.blockchain.eth

import com.tudb.blockchain.eth.entity.EthTransaction
import com.tudb.tools.HexStringUtils
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}

import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
class EthTransactionImporter(db: RocksDB, tokenId: Int) {
  // rocksdb
  val writeOptions = new WriteOptions()
  writeOptions.setDisableWAL(false)
  val writeBatch = new WriteBatch()

  // key, money
  val outTxArray = ArrayBuffer[(Array[Byte], Array[Byte])]()
  val inTxArray = ArrayBuffer[(Array[Byte], Array[Byte])]()

  def importer(txs: Seq[EthTransaction]): Unit = {
    txs.foreach(tx => {
      val fromAddress = HexStringUtils.removeHexStringHeader(tx.from)
      val toAddress = HexStringUtils.removeHexStringHeader(tx.to)
      val money = HexStringUtils.removeHexStringHeader(tx.money)
      val timestamp = ~tx.timestamp // negation long
      val txHash = HexStringUtils.removeHexStringHeader(tx.txHash)

      val key =
        EthKeyConverter.toTransactionKeyBytes(tokenId, fromAddress, toAddress, timestamp, txHash)

      val value = HexStringUtils.hexString2ArrayBytes(money)

      outTxArray.append((key.outKey, value))
      inTxArray.append((key.inKey, value))
    })

    outTxArray.foreach(kv => writeBatch.put(kv._1, kv._2))
    inTxArray.foreach(kv => writeBatch.put(kv._1, kv._2))

    outTxArray.clear()
    inTxArray.clear()

    db.write(writeOptions, writeBatch)
    writeBatch.clear()
  }
}
