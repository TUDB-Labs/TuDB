package com.tudb.blockchain

import com.tudb.blockchain.entities.ResponseTransaction
import com.tudb.storage.meta.MetaStoreApi
import com.tudb.tools.ByteUtils
import com.tudb.tools.HexStringUtils.{arrayBytes2HexString, hexString2ArrayBytes, removeHexStringHeader}
import org.rocksdb.{ReadOptions, RocksDB}

/**
  *@description:
  */
class BlockchainQueryApi(chainDB: RocksDB, metaStoreApi: MetaStoreApi) {
  def findAllOutTransactions(): Iterator[ResponseTransaction] = {
    val prefix = Array('o'.toByte)
    new TransactionPrefixIterator(prefix, chainDB).map(kv => {
      val key = kv._1
      val from = "0x" + arrayBytes2HexString(key.slice(1, 21))
      val to = "0x" + arrayBytes2HexString(key.slice(21, 41))
      val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
      val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
      val money = arrayBytes2HexString(kv._2)
      ResponseTransaction(from, to, tokenName, money, timestamp)
    })
  }

  def findAllInTransactions(): Iterator[ResponseTransaction] = {
    val prefix = Array('i'.toByte)
    new TransactionPrefixIterator(prefix, chainDB).map(kv => {
      val key = kv._1
      val to = "0x" + arrayBytes2HexString(key.slice(1, 21))
      val from = "0x" + arrayBytes2HexString(key.slice(21, 41))
      val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
      val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
      val money = arrayBytes2HexString(kv._2)
      ResponseTransaction(from, to, tokenName, money, timestamp)
    })
  }

  def findOutTransaction(from: String): Iterator[ResponseTransaction] = {
    val fromBytes = hexString2ArrayBytes(removeHexStringHeader(from))
    val prefix = Array('o'.toByte) ++ fromBytes

    new TransactionPrefixIterator(prefix, chainDB).map(kv => {
      val key = kv._1
      val from = "0x" + arrayBytes2HexString(key.slice(1, 21))
      val to = "0x" + arrayBytes2HexString(key.slice(21, 41))
      val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
      val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
      val money = arrayBytes2HexString(kv._2)
      ResponseTransaction(from, to, tokenName, money, timestamp)
    })
  }
  def findOutTransaction(from: String, tokenName: String): Iterator[ResponseTransaction] = {
    if (!metaStoreApi.isContainTokenName(tokenName)) return Iterator.empty
    val tokenIdBytes = new Array[Byte](4)
    ByteUtils.setInt(tokenIdBytes, 0, metaStoreApi.getTokenNameId(tokenName).get)
    val fromBytes = hexString2ArrayBytes(removeHexStringHeader(from))
    val prefix = Array('o'.toByte) ++ fromBytes

    new TransactionPrefixIteratorFilterByToken(prefix, tokenIdBytes, chainDB)
      .map(kv => {
        val key = kv._1
        val from = "0x" + arrayBytes2HexString(key.slice(1, 21))
        val to = "0x" + arrayBytes2HexString(key.slice(21, 41))
        val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
        val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
        val money = arrayBytes2HexString(kv._2)
        ResponseTransaction(from, to, tokenName, money, timestamp)
      })
  }

  def findInTransaction(to: String): Iterator[ResponseTransaction] = {
    val toBytes = hexString2ArrayBytes(removeHexStringHeader(to))
    val prefix = Array('i'.toByte) ++ toBytes

    new TransactionPrefixIterator(prefix, chainDB).map(kv => {
      val key = kv._1
      val to = "0x" + arrayBytes2HexString(key.slice(1, 21))
      val from = "0x" + arrayBytes2HexString(key.slice(21, 41))
      val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
      val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
      val money = arrayBytes2HexString(kv._2)
      ResponseTransaction(from, to, tokenName, money, timestamp)
    })
  }
  def findInTransaction(to: String, tokenName: String): Iterator[ResponseTransaction] = {
    if (!metaStoreApi.isContainTokenName(tokenName)) return Iterator.empty
    val toBytes = hexString2ArrayBytes(removeHexStringHeader(to))
    val prefix = Array('i'.toByte) ++ toBytes
    val tokenIdBytes = new Array[Byte](4)
    ByteUtils.setInt(tokenIdBytes, 0, metaStoreApi.getTokenNameId(tokenName).get)

    new TransactionPrefixIteratorFilterByToken(prefix, tokenIdBytes, chainDB).map(kv => {
      val key = kv._1
      val to = "0x" + arrayBytes2HexString(key.slice(1, 21))
      val from = "0x" + arrayBytes2HexString(key.slice(21, 41))
      val timestamp = ~ByteUtils.getLong(key.slice(41, 49), 0)
      val tokenName = metaStoreApi.getTokenName(ByteUtils.getInt(key.slice(49, 53), 0)).get
      val money = arrayBytes2HexString(kv._2)
      ResponseTransaction(from, to, tokenName, money, timestamp)
    })
  }

}

class TransactionPrefixIteratorFilterByToken(prefix: Array[Byte], tokenBytes: Array[Byte], db: RocksDB)
  extends Iterator[(Array[Byte], Array[Byte])] {
  val readOptions = new ReadOptions()
  val iter = db.newIterator()
  iter.seek(prefix)

  // filter by token
  while (iter.isValid && iter.key().startsWith(prefix) && !java.util.Arrays.equals(
           tokenBytes,
           iter.key().slice(49, 53)
         )) {
    iter.next()
  }
  override def hasNext: Boolean =
    iter.isValid && iter.key().startsWith(prefix) && java.util.Arrays.equals(tokenBytes, iter.key().slice(49, 53))

  override def next(): (Array[Byte], Array[Byte]) = {
    val keyBytes = iter.key()
    val moneyBytes = iter.value()
    iter.next()
    (keyBytes, moneyBytes)
  }
}

class TransactionPrefixIterator(prefix: Array[Byte], db: RocksDB) extends Iterator[(Array[Byte], Array[Byte])] {
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
