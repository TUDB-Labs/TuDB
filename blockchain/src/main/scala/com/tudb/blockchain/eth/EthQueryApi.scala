package com.tudb.blockchain.eth

import com.tudb.blockchain.eth.entity.ResponseTransaction
import com.tudb.storage.meta.MetaStoreApi
import com.tudb.tools.ByteUtils
import com.tudb.tools.HexStringUtils.{arrayBytes2HexString, hexString2ArrayBytes, removeHexStringHeader}
import org.rocksdb.{ReadOptions, RocksDB}

import java.math.BigInteger

/**
  *@description:
  */
class EthQueryApi(db: RocksDB, metaStoreApi: MetaStoreApi) {

  def findOutTransactions(): Iterator[ResponseTransaction] = {
    val prefix = Array('o'.toByte)
    new EthTransactionPrefixIterator(prefix, db).map(kv => {
      val key = kv._1
      val from = "0x" + arrayBytes2HexString(key.slice(1, 21))
      val to = "0x" + arrayBytes2HexString(key.slice(21, 41))
      val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
      val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
      val money = new BigInteger(arrayBytes2HexString(kv._2), 16)
      ResponseTransaction(from, to, tokenName, money, timestamp)
    })
  }

  def findInTransactions(): Iterator[ResponseTransaction] = {
    val prefix = Array('i'.toByte)
    new EthTransactionPrefixIterator(prefix, db).map(kv => {
      val key = kv._1
      val to = "0x" + arrayBytes2HexString(key.slice(1, 21))
      val from = "0x" + arrayBytes2HexString(key.slice(21, 41))
      val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
      val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
      val money = new BigInteger(arrayBytes2HexString(kv._2), 16)
      ResponseTransaction(from, to, tokenName, money, timestamp)
    })
  }

  def findOutTransactionByAddress(from: String): Iterator[ResponseTransaction] = {
    val fromBytes = hexString2ArrayBytes(removeHexStringHeader(from))
    val prefix = Array('o'.toByte) ++ fromBytes

    new EthTransactionPrefixIterator(prefix, db).map(kv => {
      val key = kv._1
      val from = "0x" + arrayBytes2HexString(key.slice(1, 21))
      val to = "0x" + arrayBytes2HexString(key.slice(21, 41))
      val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
      val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
      val money = new BigInteger(arrayBytes2HexString(kv._2), 16)
      ResponseTransaction(from, to, tokenName, money, timestamp)
    })
  }

  def findInTransactionsByAddress(to: String): Iterator[ResponseTransaction] = {
    val toBytes = hexString2ArrayBytes(removeHexStringHeader(to))
    val prefix = Array('i'.toByte) ++ toBytes

    new EthTransactionPrefixIterator(prefix, db).map(kv => {
      val key = kv._1
      val to = "0x" + arrayBytes2HexString(key.slice(1, 21))
      val from = "0x" + arrayBytes2HexString(key.slice(21, 41))
      val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
      val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
      val money = new BigInteger(arrayBytes2HexString(kv._2), 16)
      ResponseTransaction(from, to, tokenName, money, timestamp)
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
