package com.tudb.blockchain.eth

import com.tudb.blockchain.eth.synchronizer.EthBlockChainSynchronizer
import com.tudb.blockchain.storage.RocksDBStorageConfig
import org.apache.commons.io.FileUtils
import org.junit.{After, Before, Test}
import org.rocksdb.RocksDB

import java.io.File

/**
  *@description:
  */
class TestBackgroundPullData {
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
  def testBackgroundPullData(): Unit = {
    val synchronizer = new EthBlockChainSynchronizer(db, "192.168.31.178", 8546)
    synchronizer.synchronizeBlockChainData()
    Thread.sleep(100000)
  }

  @After
  def close(): Unit = {
    db.close()
  }
}
