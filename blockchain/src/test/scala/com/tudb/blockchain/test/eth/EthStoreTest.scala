package com.tudb.blockchain.test.eth

import com.tudb.blockchain.{BlockchainQueryApi, TokenNames}
import com.tudb.blockchain.entities.{EthTransaction, ResponseTransaction}
import com.tudb.blockchain.importer.{BlockchainTransactionImporter}
import com.tudb.storage.RocksDBStorageConfig
import com.tudb.storage.meta.MetaStoreApi
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}
import org.rocksdb.RocksDB

import java.io.File
import java.math.BigInteger

/**
  *@description:
  */
class EthStoreTest {
  val dbPath = "./testdata/testEth"

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

  val token1: String = TokenNames.ETHEREUM_NATIVE_COIN
  val token2: String = TokenNames.USDT
  val token3: String = TokenNames.USDC

  @Before
  def init(): Unit = {
    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    file.mkdirs()
  }

  @Test
  def testEthStore(): Unit = {
    val tx1 = EthTransaction(address1, address2, token1, money1, timestamp1, txHash1)
    val response1 =
      ResponseTransaction(address1, address2, token1, new BigInteger(money1, 16), timestamp1)

    val tx2 = EthTransaction(address3, address4, token2, money2, timestamp2, txHash2)
    val response2 =
      ResponseTransaction(address3, address4, token2, new BigInteger(money2, 16), timestamp2)

    val tx3 = EthTransaction(address5, address6, token3, money3, timestamp3, txHash3)
    val response3 =
      ResponseTransaction(address5, address6, token3, new BigInteger(money3, 16), timestamp3)

    val blockchain = "ethereum"
    val importer = new BlockchainTransactionImporter(dbPath, blockchain)

    importer.importer(Seq(tx1, tx2, tx3))
    importer.close()
    val chainDB =
      RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/${blockchain}.db")
    val metaDB = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/meta.db")
    val metaStoreApi = new MetaStoreApi(metaDB)
    val queryApi = new BlockchainQueryApi(chainDB, metaStoreApi)

    val txArrayOut = queryApi.findOutTransactions().toSeq
    val txArrayIn = queryApi.findInTransactions().toSeq
    val groundTruth = Seq(response1, response2, response3)

    Assert.assertEquals(3, txArrayOut.length)
    Assert.assertTrue(groundTruth.toSet == txArrayOut.toSet)

    Assert.assertEquals(3, txArrayIn.length)
    Assert.assertTrue(groundTruth.toSet == txArrayIn.toSet)

    chainDB.close()
    metaDB.close()
  }

  @Test
  def testSortByLatestTimestamp(): Unit = {
    val tx1 = EthTransaction(address1, address2, token1, money1, timestamp1, txHash1)
    val tx2 = EthTransaction(address1, address2, token2, money2, timestamp2, txHash2)
    val tx3 = EthTransaction(address1, address2, token3, money3, timestamp3, txHash3)

    val response1 =
      ResponseTransaction(address1, address2, token1, new BigInteger(money1, 16), timestamp1)
    val response2 =
      ResponseTransaction(address1, address2, token2, new BigInteger(money2, 16), timestamp2)
    val response3 =
      ResponseTransaction(address1, address2, token3, new BigInteger(money3, 16), timestamp3)

    val blockchain = "ethereum"
    val importer = new BlockchainTransactionImporter(dbPath, blockchain)

    importer.importer(Seq(tx1, tx2, tx3))
    importer.close()
    val chainDB =
      RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/${blockchain}.db")
    val metaDB = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/meta.db")
    val metaStoreApi = new MetaStoreApi(metaDB)
    val queryApi = new BlockchainQueryApi(chainDB, metaStoreApi)

    val queryOutResult = queryApi.findOutTransactionByAddress(address1).toSeq
    val queryInResult = queryApi.findInTransactionsByAddress(address2).toSeq
    val groundTruth = Seq(response3, response2, response1)

    Assert.assertEquals(groundTruth, queryOutResult)
    Assert.assertEquals(groundTruth, queryInResult)

    chainDB.close()
    metaDB.close()
  }
}
