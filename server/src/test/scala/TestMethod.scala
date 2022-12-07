import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.tudb.blockchain.eth.client.EthClientApi
import com.tudb.blockchain.eth.meta.MetaKeyManager
import com.tudb.blockchain.server.JSONAddress
import com.tudb.blockchain.storage.{QueryApi, RocksDBStorageConfig}
import com.tudb.blockchain.tools.ByteUtils
import org.rocksdb.RocksDB

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

/**
  *@description:
  */
object TestMethod {
  def main(args: Array[String]): Unit = {
    val db = RocksDB.open(
      RocksDBStorageConfig.getDefault(true),
      "/Users/gaolianxin/Desktop/coding/20221201_tudb/TuDB/server/testdata/test.db"
    )
    val queryApi = new QueryApi(db)
    queryApi.allAddress().foreach(println)
    db.close()
  }
}
