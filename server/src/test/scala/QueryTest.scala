import com.tudb.blockchain.BlockchainQueryApi
import com.tudb.storage.RocksDBStorageConfig
import com.tudb.storage.meta.MetaStoreApi
import org.junit.Test
import org.rocksdb.RocksDB

/**
  *@description:
  */
class QueryTest {
  val dbPath = "/Users/gaolianxin/Desktop/coding/20221201_tudb/TuDB/testdata"

  @Test
  def testQuery(): Unit = {
    val chainDB =
      RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/ethereum.db")
    val metaDB = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/meta.db")
    val metaStoreApi = new MetaStoreApi(metaDB)

    val queryApi = new BlockchainQueryApi(chainDB, metaStoreApi)
    println(queryApi.findAllOutTransactions().length)

    queryApi
      .findOutTransaction("0x912fd21d7a69678227fe6d08c64222db41477ba0")
      .foreach(println)
//    queryApi.findOutTransactions().foreach(println)
  }
}
