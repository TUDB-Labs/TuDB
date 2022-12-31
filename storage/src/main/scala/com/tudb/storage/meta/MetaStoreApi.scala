package com.tudb.storage.meta

import com.tudb.storage.tools.MetaKeyConverter
import com.tudb.tools.ByteUtils
import org.rocksdb.RocksDB

/**
  *@description:
  */
class MetaStoreApi(metaDB: RocksDB) {
  val metaNameStore = new MetaNameStore(metaDB)

  def getAllBlockchainNames(): Seq[String] = {
    metaNameStore.chainName2Id.keys.toSeq
  }

  def setSynchronizedBlockNumber(blockchain: String, blockNumber: Long): Unit = {
    val key = MetaKeyConverter.getSynchronizedBlockNumberKey(blockchain)
    val longBytes: Array[Byte] = new Array[Byte](8)
    ByteUtils.setLong(longBytes, 0, blockNumber)
    metaDB.put(key, longBytes)
  }

  def getSynchronizedBlockNumber(blockchain: String): Long = {
    val key = MetaKeyConverter.getSynchronizedBlockNumberKey(blockchain)
    val number = metaDB.get(key)
    if (number != null) ByteUtils.getLong(number, 0)
    else -1
  }

  def addBlockchainNameMeta(blockchain: String): Unit = {
    metaNameStore.addChainNameToDB(blockchain)
  }
  def getBlockchainNameId(blockchain: String): Option[Int] = {
    metaNameStore.getChainId(blockchain)
  }
  def getBlockchainName(blockchainId: Int): Option[String] = {
    metaNameStore.getChainName(blockchainId)
  }
  def getOrAddChainName(blockchain: String): Int = {
    metaNameStore.getOrAddChainName(blockchain)
  }

  def addTokenNameMeta(token: String): Unit = {
    metaNameStore.addTokenNameToDB(token)
  }
  def getTokenNameId(token: String): Option[Int] = {
    metaNameStore.getTokenId(token)
  }
  def getTokenName(tokenId: Int): Option[String] = {
    metaNameStore.getTokenName(tokenId)
  }
  def getOrAddTokenName(tokenName: String): Int = {
    metaNameStore.getOrAddTokenName(tokenName)
  }
}
