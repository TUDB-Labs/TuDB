package com.tudb.blockchain.eth.importer

import com.alibaba.fastjson.JSONObject
import com.tudb.blockchain.eth.{EthJsonParser, EthKeyConverter}
import com.tudb.blockchain.tools.DataConverter
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
class TransactionImporter(
    db: RocksDB,
    msgQueue: ConcurrentLinkedQueue[JSONObject],
    countTransaction: AtomicLong) {
  // rocksdb
  val writeOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  val writeBatch = new WriteBatch()

  val fromAddressLabelArray = ArrayBuffer[Array[Byte]]()
  val fromLabelAddressArray = ArrayBuffer[Array[Byte]]()
  val toAddressLabelArray = ArrayBuffer[Array[Byte]]()
  val toLabelAddressArray = ArrayBuffer[Array[Byte]]()
  val outTxArray = ArrayBuffer[(Array[Byte], Array[Byte])]()
  val inTxArray = ArrayBuffer[(Array[Byte], Array[Byte])]()

  def importer(): Unit = {
    val blockJson = msgQueue.poll()
    val txs = EthJsonParser.getBlockTransaction(blockJson)
    countTransaction.addAndGet(txs.length)
    txs.foreach(tx => {
      val innerFrom = DataConverter.removeHexStringHeader(tx.from)
      val innerTo = DataConverter.removeHexStringHeader(tx.to)
      val innerTxHash = DataConverter.removeHexStringHeader(tx.txHash)
      val innerWei = DataConverter.removeHexStringHeader(tx.wei)

      val fromKeys = EthKeyConverter.toAddressKey(innerFrom)
      val toKeys = EthKeyConverter.toAddressKey(innerTo)
      val txKeys = EthKeyConverter.toTransactionKey(innerFrom, innerTo, innerTxHash)
      val txWei = DataConverter.hexString2ArrayBytes(innerWei)

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
  }
}
