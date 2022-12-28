import com.tudb.storage.RocksDBStorageConfig
import com.tudb.storage.meta.MetaNameStore
import org.apache.commons.io.FileUtils
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.RocksDB

import java.io.File

/**
  *@description:
  */
class MetaNameStoreTest {
  val dbPath = "./testdata/testEth.db"
  var metaNameStore: MetaNameStore = _
  @Before
  def init(): Unit = {
    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    file.mkdirs()
    val db = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), dbPath)
    metaNameStore = new MetaNameStore(db)
  }

  @Test
  def chainMetaTest(): Unit = {
    Assert.assertEquals(0, metaNameStore.chainId2Name.size)
    Assert.assertEquals(0, metaNameStore.chainName2Id.size)

    metaNameStore.addChainNameToDB("BitCoin")
    metaNameStore.addChainNameToDB("Ethereum")
    Assert.assertEquals(Some(0), metaNameStore.getChainId("BitCoin"))
    Assert.assertEquals(Some(1), metaNameStore.getChainId("Ethereum"))

    metaNameStore.loadChainMeta()
    Assert.assertEquals(Some(0), metaNameStore.getChainId("BitCoin"))
    Assert.assertEquals(Some(1), metaNameStore.getChainId("Ethereum"))
  }

  @Test
  def tokenMetaTest(): Unit = {
    Assert.assertEquals(0, metaNameStore.tokenName2Id.size)
    Assert.assertEquals(0, metaNameStore.tokenId2Name.size)

    metaNameStore.addTokenNameToDB("USDT")
    metaNameStore.addTokenNameToDB("USDC")
    Assert.assertEquals(Some(0), metaNameStore.getTokenId("USDT"))
    Assert.assertEquals(Some(1), metaNameStore.getTokenId("USDC"))

    metaNameStore.loadTokenMeta()
    Assert.assertEquals(Some(0), metaNameStore.getTokenId("USDT"))
    Assert.assertEquals(Some(1), metaNameStore.getTokenId("USDC"))
  }

  @After
  def close(): Unit = {
    metaNameStore.close()
  }
}
