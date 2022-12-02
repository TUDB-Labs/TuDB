package com.tudb.blockchain.storage

import com.tudb.blockchain.eth.EthKeyConverter
import com.tudb.blockchain.tools.DataConverter
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.{FlushOptions, RocksDB}

import java.io.File

/**
  *@description:
  */
class KeyStoreTest {
  val dbPath = "./testdata/test.db"
  var db: RocksDB = _
  @Before
  def init(): Unit = {
    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    file.mkdirs()

    db = RocksDB.open(RocksDBStorageConfig.getDefault(true), dbPath)
  }

  @Test
  def testStorage(): Unit = {
    val timestamp1 = DataConverter.hexString2Long2Bytes("63895da0")
    val timestamp2 = DataConverter.hexString2Long2Bytes("63895da1")
    val timestamp3 = DataConverter.hexString2Long2Bytes("63895da2")

    db.put(timestamp3, Array(1.toByte))
    db.put(timestamp2, Array(2.toByte))
    db.put(timestamp1, Array(3.toByte))

    val iter = db.newIterator()
    iter.seekToFirst()
    while (iter.isValid) {
      val next = iter.key()
      val data = DataConverter.arrayBytes2Long2hexString(next)
      println(data)
      iter.next()
    }

  }

  @Test
  def testGetTransactionOrderByTimeStamp: Unit = {

    val fromAddress = "f1f083b3a12Ca65FE906B3Ebe3A5aB0d79A2b19D"
    val toAddress = "f1f083b3a12Ca65FE906B3Ebe3A5aB0d79A2b110"

    val timestamp1 = "63895da0"
    val timestamp2 = "63895da1"
    val timestamp3 = "63895da2"
    val timestamp4 = "63895da3"

    val txHash = "885f1ffa783ab5dbbede44f6a5801bf8fc6342da37ec7e0ca2a89d9eb9437266"

    val key1 = EthKeyConverter.toTransactionKey(fromAddress, toAddress, timestamp1, txHash)._1
    val key2 = EthKeyConverter.toTransactionKey(fromAddress, toAddress, timestamp2, txHash)._1
    val key3 = EthKeyConverter.toTransactionKey(fromAddress, toAddress, timestamp3, txHash)._1
    val key4 = EthKeyConverter.toTransactionKey(fromAddress, toAddress, timestamp4, txHash)._1
    db.put(key1, Array.emptyByteArray)
    db.put(key2, Array.emptyByteArray)
    db.put(key3, Array.emptyByteArray)
    db.put(key4, Array.emptyByteArray)

    val queryApi = new QueryApi(db)
    val res = queryApi
      .innerFindOutKey(DataConverter.hexString2ArrayBytes(fromAddress))
      .toSeq
      .map(f => DataConverter.arrayBytes2Long2hexString(f.slice(43, 51)))

    Assert.assertTrue(res == Seq(timestamp4, timestamp3, timestamp2, timestamp1))
  }

  @After
  def close(): Unit = {
    db.close()
  }
}
