import com.tudb.storage.RocksDBStorageConfig
import com.tudb.storage.meta.{MetaNameStore, MetaStoreApi}
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}
import org.rocksdb.RocksDB

import java.io.File

/**
  *@description:
  */
class MetaApiTest {
  val dbPath = "./testdata/testEth.db"
  var metaStoreApi: MetaStoreApi = _

  @Before
  def init(): Unit = {
    val file = new File(dbPath)
    FileUtils.deleteDirectory(file)
    file.mkdirs()
    val db = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), dbPath)
    metaStoreApi = new MetaStoreApi(db)
  }

  @Test
  def testSynchronizedBlockNumber(): Unit = {
    metaStoreApi.setSynchronizedBlockNumber("Ethereum", 2333333)
    val blockNumber = metaStoreApi.getSynchronizedBlockNumber("Ethereum")
    Assert.assertEquals(2333333, blockNumber)
  }
}
