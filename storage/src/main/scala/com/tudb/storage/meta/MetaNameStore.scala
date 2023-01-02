package com.tudb.storage.meta

import com.tudb.blockchain.tools.ByteUtils
import com.tudb.storage.tools.MetaKeyConverter
import org.rocksdb.RocksDB

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/**
  *@description:
  */
class MetaNameStore(db: RocksDB) {
  val charset = "UTF-8"
  val chainIdGenerator = new AtomicInteger(0)
  val tokenIdGenerator = new AtomicInteger(0)

  val chainName2Id: mutable.Map[String, Int] = mutable.Map[String, Int]()
  val chainId2Name: mutable.Map[Int, String] = mutable.Map[Int, String]()

  val tokenName2Id: mutable.Map[String, Int] = mutable.Map[String, Int]()
  val tokenId2Name: mutable.Map[Int, String] = mutable.Map[Int, String]()

  loadChainMeta()
  loadTokenMeta()

  def addChainNameToDB(name: String): Int = {
    val id = chainIdGenerator.incrementAndGet()
    chainName2Id += name -> id
    chainId2Name += id -> name
    val key = MetaKeyConverter.getBlockchainKey(id)
    db.put(key, name.getBytes(charset))
    id
  }
  def addTokenNameToDB(name: String): Int = {
    val id = tokenIdGenerator.incrementAndGet()
    tokenName2Id += name -> id
    tokenId2Name += id -> name
    val key = MetaKeyConverter.getTokenKey(id)
    db.put(key, name.getBytes(charset))
    id
  }

  def getChainName(chainId: Int): Option[String] = chainId2Name.get(chainId)
  def getChainId(chainName: String): Option[Int] = chainName2Id.get(chainName)

  def getTokenName(tokenId: Int): Option[String] = tokenId2Name.get(tokenId)
  def getTokenId(tokenName: String): Option[Int] = tokenName2Id.get(tokenName)

  def getOrAddChainName(chainName: String): Int =
    chainName2Id.getOrElse(chainName, addChainNameToDB(chainName))
  def getOrAddTokenName(tokenName: String): Int =
    tokenName2Id.getOrElse(tokenName, addTokenNameToDB(tokenName))

  def loadChainMeta(): Unit = {
    chainName2Id.clear()
    chainId2Name.clear()

    val iterator = db.newIterator()
    val prefix = MetaKeyConverter.chainType
    iterator.seek(prefix)
    while (iterator.isValid && iterator.key().startsWith(prefix)) {
      val id = ByteUtils.getInt(iterator.key().drop(1), 0)
      val chainName = new String(iterator.value(), charset)
      chainName2Id += chainName -> id
      chainId2Name += id -> chainName
      iterator.next()
    }
    chainIdGenerator.set(chainName2Id.size)
  }
  def loadTokenMeta(): Unit = {
    tokenName2Id.clear()
    tokenId2Name.clear()

    val iterator = db.newIterator()
    val prefix = MetaKeyConverter.tokenType
    iterator.seek(prefix)
    while (iterator.isValid && iterator.key().startsWith(prefix)) {
      val id = ByteUtils.getInt(iterator.key().drop(1), 0)
      val tokenName = new String(iterator.value(), charset)
      tokenName2Id += tokenName -> id
      tokenId2Name += id -> tokenName
      iterator.next()
    }
    tokenIdGenerator.set(tokenName2Id.size)
  }

  def close(): Unit = {
    db.close()
  }
}
