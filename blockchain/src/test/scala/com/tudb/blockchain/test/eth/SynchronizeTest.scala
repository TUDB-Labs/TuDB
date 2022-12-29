package com.tudb.blockchain.test.eth

import com.tudb.blockchain.BlockchainQueryApi
import com.tudb.blockchain.eth.{EthBlockchainSynchronizer, Web3jEthClient}
import com.tudb.storage.RocksDBStorageConfig
import com.tudb.storage.meta.MetaStoreApi
import org.apache.commons.io.FileUtils
import org.junit.{Before, Test}
import org.rocksdb.RocksDB

import java.io.File

/**
  *@description:
  */
class SynchronizeTest {
  val dbPath = "./testdata/testEth"
  @Before
  def init(): Unit = {
    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    file.mkdirs()
  }

  @Test
  def testSynchronize(): Unit = {
    val web3jEthClient = new Web3jEthClient(
      "https://mainnet.infura.io/v3/6afd74985a834045af3d4d8f6344730a"
    )
    val chainDB =
      RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/ethereum.db")
    val metaDB = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/meta.db")
    val metaStoreApi = new MetaStoreApi(metaDB)
    val synchronizer = new EthBlockchainSynchronizer(web3jEthClient, chainDB, metaStoreApi)
    synchronizer.start()
    Thread.sleep(1000)
    synchronizer.stop()

    val queryApi = new BlockchainQueryApi(chainDB, metaStoreApi)
    println(queryApi.findOutTransactions().length)
    println(queryApi.findInTransactions().length)
  }
}
