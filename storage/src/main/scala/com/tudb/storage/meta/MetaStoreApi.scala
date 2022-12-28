package com.tudb.storage.meta

import com.tudb.storage.tools.MetaKeyConverter
import com.tudb.tools.ByteUtils
import org.rocksdb.RocksDB

/**
  *@description:
  */
class MetaStoreApi(db: RocksDB) {
  val metaNameStore = new MetaNameStore(db)

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

  def addBlockchainNameMeta(blockchain: String): Unit = {
    metaNameStore.addChainNameToDB(blockchain)
  }

  def getBlockchainNameId(blockchain: String): Option[Int] = {
    metaNameStore.getChainId(blockchain)
  }

  def addTokenNameMeta(token: String): Unit = {
    metaNameStore.addTokenNameToDB(token)
  }

  def getTokenNameId(token: String): Option[Int] = {
    metaNameStore.getTokenId(token)
  }
}
