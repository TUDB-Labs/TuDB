//import com.tudb.blockchain.storage.{QueryApi, RocksDBStorageConfig}
//import org.rocksdb.RocksDB
//
///**
//  *@description:
//  */
//object TestMethod {
//  def main(args: Array[String]): Unit = {
//    val db = RocksDB.open(
//      RocksDBStorageConfig.getDefault(true),
//      "/Users/gaolianxin/Desktop/coding/20221201_tudb/TuDB/server/testdata/test.db"
//    )
//    val queryApi = new QueryApi(db)
//    queryApi.allAddress().foreach(println)
//    db.close()
//  }
//}
