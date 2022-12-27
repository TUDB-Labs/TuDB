package com.tudb.storage.meta

import com.tudb.storage.tools.MetaKeyConverter
import com.tudb.tools.ByteUtils
import org.rocksdb.RocksDB

/**
  *@description:
  */
class MetaStoreApi(db: RocksDB) {

  def setSynchronizedBlockNumber(blockchain: String, blockNumber: Long): Unit = {
    val key = MetaKeyConverter.getSynchronizedBlockNumberKey(blockchain)
    val longBytes: Array[Byte] = new Array[Byte](8)
    ByteUtils.setLong(longBytes, 0, blockNumber)
    db.put(key, longBytes)
  }

  def getSynchronizedBlockNumber(blockchain: String): Long = {
    val key = MetaKeyConverter.getSynchronizedBlockNumberKey(blockchain)
    val number = db.get(key)
    if (number != null) ByteUtils.getLong(number, 0)
    else -1
  }

  def setBlockchainNameId(blockchain: String, id: Int): Unit = {
    val key = MetaKeyConverter.getBlockchainKey(blockchain)
    val intBytes = new Array[Byte](4)
    ByteUtils.setInt(intBytes, 0, id)
    db.put(key, intBytes)
  }

  def getBlockchainNameId(blockchain: String): Int = {
    val key = MetaKeyConverter.getBlockchainKey(blockchain)
    val id = db.get(key)
    if (id != null) ByteUtils.getInt(id, 0)
    else 0
  }

  def setTokenNameId(token: String, id: Int): Unit = {
    val key = MetaKeyConverter.getTokenKey(token)
    val intBytes = new Array[Byte](4)
    ByteUtils.setInt(intBytes, 0, id)
    db.put(key, intBytes)
  }

  def getTokenNameId(token: String): Int = {
    val key = MetaKeyConverter.getTokenKey(token)
    val id = db.get(key)
    if (id != null) ByteUtils.getInt(id, 0)
    else 0
  }

  def setLabelNameId(label: String, id: Int): Unit = {
    val key = MetaKeyConverter.getLabelKey(label)
    val intBytes = new Array[Byte](4)
    ByteUtils.setInt(intBytes, 0, id)
    db.put(key, intBytes)
  }
  def getLabelNameId(label: String): Int = {
    val key = MetaKeyConverter.getLabelKey(label)
    val id = db.get(key)
    if (id != null) ByteUtils.getInt(id, 0)
    else 0
  }
}
