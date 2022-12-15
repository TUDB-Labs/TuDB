package com.tudb.blockchain.eth.synchronizer

import com.alibaba.fastjson.JSONObject
import com.tudb.blockchain.eth.EthKeyConverter
import com.tudb.blockchain.eth.client.{EthClientApi, EthJsonObjectParser, EthTransaction}
import com.tudb.blockchain.tools.DataConverter
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
class TransactionImporter(db: RocksDB) {
  // rocksdb
  val writeOptions = new WriteOptions()
  writeOptions.setDisableWAL(false)
  val writeBatch = new WriteBatch()

  val fromAddressLabelArray = ArrayBuffer[Array[Byte]]()
  val fromLabelAddressArray = ArrayBuffer[Array[Byte]]()
  val toAddressLabelArray = ArrayBuffer[Array[Byte]]()
  val toLabelAddressArray = ArrayBuffer[Array[Byte]]()
  val outTxArray = ArrayBuffer[(Array[Byte], Array[Byte])]()
  val inTxArray = ArrayBuffer[(Array[Byte], Array[Byte])]()

  def importer(txs: Array[EthTransaction]): (Boolean, Int) = {
    val hasData: Boolean = !txs.isEmpty
    val dataSize = txs.length
    txs.foreach(tx => {
      val innerFrom = DataConverter.removeHexStringHeader(tx.from)
      val innerTo = DataConverter.removeHexStringHeader(tx.to)
      val innerTxHash = DataConverter.removeHexStringHeader(tx.txHash)
      val innerWei = DataConverter.removeHexStringHeader(tx.wei)
      val innerTimeStamp = DataConverter.removeHexStringHeader(tx.timeStamp)

      val fromKeys = EthKeyConverter.toAddressKey(innerFrom)
      val toKeys = EthKeyConverter.toAddressKey(innerTo)
      val txKeys = EthKeyConverter.toTransactionKey(innerFrom, innerTo, innerTimeStamp, innerTxHash)
      val txWei = DataConverter.hexString2ArrayBytes(innerWei)

      fromAddressLabelArray.append(fromKeys._1)
      fromLabelAddressArray.append(fromKeys._2)
      toAddressLabelArray.append(toKeys._1)
      toLabelAddressArray.append(toKeys._2)
      outTxArray.append((txKeys._1, txWei))
      inTxArray.append((txKeys._2, txWei))
    })

    fromAddressLabelArray.foreach(key => writeBatch.put(key, Array.emptyByteArray))
    fromLabelAddressArray.foreach(key => writeBatch.put(key, Array.emptyByteArray))
    toAddressLabelArray.foreach(key => writeBatch.put(key, Array.emptyByteArray))
    toLabelAddressArray.foreach(key => writeBatch.put(key, Array.emptyByteArray))
    outTxArray.foreach(kv => writeBatch.put(kv._1, kv._2))
    inTxArray.foreach(kv => writeBatch.put(kv._1, kv._2))

    fromAddressLabelArray.clear()
    fromLabelAddressArray.clear()
    toAddressLabelArray.clear()
    toLabelAddressArray.clear()
    outTxArray.clear()
    inTxArray.clear()

    db.write(writeOptions, writeBatch)
    writeBatch.clear()

    (hasData, dataSize)
  }
}
