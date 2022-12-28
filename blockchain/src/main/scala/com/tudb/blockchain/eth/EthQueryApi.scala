package com.tudb.blockchain.eth

import com.tudb.blockchain.eth.entity.ResponseTransaction
import com.tudb.tools.ByteUtils
import com.tudb.tools.HexStringUtils.{hexString2ArrayBytes, removeHexStringHeader, arrayBytes2HexString}
import org.rocksdb.{ReadOptions, RocksDB}

import java.math.BigInteger

/**
  *@description:
  */
class EthQueryApi(db: RocksDB) {

  def findOutTransactions(): Iterator[ResponseTransaction] = {
    val prefix = Array('o'.toByte)
    new EthTransactionPrefixIterator(prefix, db).map(kv => {
      val key = kv._1
      val from = "0x" + arrayBytes2HexString(key.slice(5, 25))
      val to = "0x" + arrayBytes2HexString(key.slice(25, 45))
      val timestamp = ~ByteUtils.getLong(key.slice(45, 53), 0)
      val money = new BigInteger(arrayBytes2HexString(kv._2), 16)
      ResponseTransaction(from, to, money, timestamp)
    })
  }

  def findInTransactions(): Iterator[ResponseTransaction] = {
    val prefix = Array('i'.toByte)
    new EthTransactionPrefixIterator(prefix, db).map(kv => {
      val key = kv._1
      val to = "0x" + arrayBytes2HexString(key.slice(5, 25))
      val from = "0x" + arrayBytes2HexString(key.slice(25, 45))
      val timestamp = ~ByteUtils.getLong(key.slice(45, 53), 0)
      val money = new BigInteger(arrayBytes2HexString(kv._2), 16)
      ResponseTransaction(from, to, money, timestamp)
    })
  }

  def findOutTransactionByAddress(tokenId: Int, from: String): Iterator[ResponseTransaction] = {
    val tokenBytes = new Array[Byte](4)
    ByteUtils.setInt(tokenBytes, 0, tokenId)

    val fromBytes = hexString2ArrayBytes(removeHexStringHeader(from))
    val prefix = Array('o'.toByte) ++ tokenBytes ++ fromBytes

    new EthTransactionPrefixIterator(prefix, db).map(kv => {
      val key = kv._1
      val from = "0x" + arrayBytes2HexString(key.slice(5, 25))
      val to = "0x" + arrayBytes2HexString(key.slice(25, 45))
      val timestamp = ~ByteUtils.getLong(key.slice(45, 53), 0)
      val money = new BigInteger(arrayBytes2HexString(kv._2), 16)
      ResponseTransaction(from, to, money, timestamp)
    })
  }

  def findInTransactionsByAddress(tokenId: Int, to: String): Iterator[ResponseTransaction] = {
    val tokenBytes = new Array[Byte](4)
    ByteUtils.setInt(tokenBytes, 0, tokenId)

    val toBytes = hexString2ArrayBytes(removeHexStringHeader(to))
    val prefix = Array('i'.toByte) ++ tokenBytes ++ toBytes

    new EthTransactionPrefixIterator(prefix, db).map(kv => {
      val key = kv._1
      val to = "0x" + arrayBytes2HexString(key.slice(5, 25))
      val from = "0x" + arrayBytes2HexString(key.slice(25, 45))
      val timestamp = ~ByteUtils.getLong(key.slice(45, 53), 0)
      val money = new BigInteger(arrayBytes2HexString(kv._2), 16)
      ResponseTransaction(from, to, money, timestamp)
    })
  }
}

class EthTransactionPrefixIterator(prefix: Array[Byte], db: RocksDB)
  extends Iterator[(Array[Byte], Array[Byte])] {
  val readOptions = new ReadOptions()
  val iter = db.newIterator()
  iter.seek(prefix)

  override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

  override def next(): (Array[Byte], Array[Byte]) = {
    val keyBytes = iter.key()
    val moneyBytes = iter.value()
    iter.next()
    (keyBytes, moneyBytes)
  }
}
