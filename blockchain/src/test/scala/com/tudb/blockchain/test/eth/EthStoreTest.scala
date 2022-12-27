package com.tudb.blockchain.test.eth

import com.tudb.blockchain.eth.{EthKeyConverter, EthTransactionImporter}
import com.tudb.blockchain.eth.entity.EthTransaction
import com.tudb.storage.RocksDBStorageConfig
import com.tudb.tools.HexStringUtils
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}
import org.rocksdb.RocksDB

import java.io.File
import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
class EthStoreTest {
  val dbPath = "./testdata/testEth.db"
  var db: RocksDB = _

  val address1 = "0xd64137f743432392538a8f84e8e571fa09f21c37"
  val address2 = "0x499d1b178b4643c12e3cf99d5b0244e9a754ee2d"
  val address3 = "0x080086911d8c78008800fae75871a657b77d0082"
  val address4 = "0xdb306e5c24cd28a02b50c6f893d46a3572835195"
  val address5 = "0x276cdba3a39abf9cedba0f1948312c0681e6d5fd"
  val address6 = "0xcac725bef4f114f728cbcfd744a731c2a463c3fc"

  val money1 = "0"
  val money2 = "f854c8"
  val money3 = "2386f26fc10000"

  val txHash1 = "0xb02190b9047048dd6be55ffc6d6df7696c0b6ae3f6f10133e182ee4f45547308"
  val txHash2 = "0x5acd9539565b4d9c2f8138e691c342c20b0b75d7c07c2fccef474409dc0b3c70"
  val txHash3 = "0x2a422e4fa302eef8e80a21156d9498fc069eef111d12f7cd78113f56f60e2534"

  val timestamp1 = 1672126595
  val timestamp2 = 1672126596
  val timestamp3 = 1672126597

  @Before
  def init(): Unit = {
    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    file.mkdirs()
    db = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), dbPath)
  }

  @Test
  def testEthStore(): Unit = {
    val tx1 = EthTransaction(address1, address2, money1, timestamp1, txHash1)
    val tx2 = EthTransaction(address3, address4, money2, timestamp2, txHash2)
    val tx3 = EthTransaction(address5, address6, money3, timestamp3, txHash3)

    val importer = new EthTransactionImporter(db, 0)
    importer.importer(Seq(tx1, tx2, tx3))

    val iter1 = db.newIterator()
    val txArray = ArrayBuffer[EthTransaction]()
    iter1.seek(Array('o'.toByte))
    while (iter1.isValid && iter1.key().startsWith(Array('o'.toByte))) {
      val txKey = EthKeyConverter.outTransactionKeyToEntity(iter1.key())
      val txMoney = HexStringUtils.arrayBytes2HexString(iter1.value())
      txArray.append(
        EthTransaction(
          "0x" + txKey.from,
          "0x" + txKey.to,
          txMoney,
          txKey.timestamp,
          "0x" + txKey.txHash
        )
      )
      iter1.next()
    }

    Assert.assertEquals(3, txArray.length)
    Assert.assertTrue(Set(tx1, tx2, tx3) == txArray.toSet)
  }
  @Test
  def testSortByLatestTimestamp(): Unit = {
    val tx1 = EthTransaction(address1, address2, money1, timestamp1, txHash1)
    val tx2 = EthTransaction(address1, address2, money2, timestamp2, txHash2)
    val tx3 = EthTransaction(address1, address2, money3, timestamp3, txHash3)

    val importer = new EthTransactionImporter(db, 0)
    importer.importer(Seq(tx1, tx2, tx3))

    val iter1 = db.newIterator()
    val txArray = ArrayBuffer[EthTransaction]()
    iter1.seek(Array('o'.toByte))
    while (iter1.isValid && iter1.key().startsWith(Array('o'.toByte))) {
      val txKey = EthKeyConverter.outTransactionKeyToEntity(iter1.key())
      val txMoney = HexStringUtils.arrayBytes2HexString(iter1.value())
      txArray.append(
        EthTransaction(
          "0x" + txKey.from,
          "0x" + txKey.to,
          txMoney,
          txKey.timestamp,
          "0x" + txKey.txHash
        )
      )
      iter1.next()
    }

    Assert.assertEquals(txArray, Seq(tx3, tx2, tx1))
  }
}
