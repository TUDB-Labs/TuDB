import com.tudb.blockchain.eth.EthBlockParser
import com.tudb.blockchain.importer.BlockchainTransactionImporter
import com.tudb.blockchain.tools.HexStringUtils
import com.tudb.storage.RocksDBStorageConfig
import com.tudb.storage.meta.MetaStoreApi
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.rocksdb.RocksDB

import java.text.SimpleDateFormat

/**
  *@description:
  */
object EthImporter {
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val spark = SparkSession
    .builder()
    .master("local[*]") // use * core
    .appName("Ethereum Daily Transaction Extract")
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.31.41:9000")
    .getOrCreate()

  def readHdfsFileNames(): Array[String] = {
    val res = FileSystem
      .get(spark.sparkContext.hadoopConfiguration)
      .listStatus(new Path("/raw/eth_transactions/"))
      .map(f => f.getPath.toString)
      .map(str => str.split("/").last)

    val latestYear = res.filter(str => str >= "blockchair_ethereum_transactions_2022")
    latestYear
  }

  def readEthDataFromSpark(fileName: String): Array[(Int, String, Long, String, String, String, String)] = {
    import spark.implicits._
    val files = spark.read
      .format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("delimiter", "\t")
      .csv(s"/raw/eth_transactions/${fileName}")
      .select("block_id", "hash", "time", "sender", "recipient", "value", "input_hex")
      .map(row =>
        (
          row.getAs[Int]("block_id"),
          row.getAs[String]("hash"),
          simpleDateFormat.parse(row.getAs[String]("time")).getTime,
          row.getAs[String]("sender"),
          row.getAs[String]("recipient"),
          row.getAs[java.math.BigDecimal]("value").toBigInteger.toString(16),
          row.getAs[String]("input_hex")
        )
      )
    files.filter(p => p._4.length > 10 && p._5.length > 10).collect().sortBy(f => f._1)
  }
  def coldImportFromSpark(dbPath: String, chainName: String): Unit = {
    val chainDB = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/${chainName}.db")
    val metaDB = RocksDB.open(RocksDBStorageConfig.getDefaultOptions(true), s"${dbPath}/meta.db")
    val metaStoreApi = new MetaStoreApi(metaDB)
    metaStoreApi.getOrAddChainName(chainName)
    val blockParser = new EthBlockParser()
    val importer = new BlockchainTransactionImporter(chainDB, metaStoreApi)

    val fileNames = Seq(readHdfsFileNames().last)
    var countImportTransaction: Long = 0
    fileNames.foreach(fileName => {
      println(s"start to import ${fileName}")
      val dayData = readEthDataFromSpark(fileName).sortBy(f => f._1)
      val groupedData = dayData.grouped(10000)
      groupedData.foreach(batch => {
        val batchData = batch
          .map(values => {
            val hash = values._2
            val timestamp = values._3
            val from = {
              if (!values._4.startsWith("0x")) "0x" + values._4
              else values._4
            }
            val to = {
              if (!values._5.startsWith("0x")) "0x" + values._5
              else values._5
            }
            val money = values._6
            val input = {
              if (values._7 == null) null
              else if (!values._7.startsWith("0x")) "0x" + values._7
              else values._7
            }
            blockParser.getBlockFromSpark(hash, from, to, timestamp, money, input)
          })
          .filter(p => p != null)
        importer.importTx(batchData)
        countImportTransaction += batchData.size
      })
      metaStoreApi.setSynchronizedBlockNumber(chainName, dayData.last._1)
      println(s"imported ${fileName}")
      println(s"imported transactions: ${countImportTransaction}")
    })
    metaStoreApi.getAllTokenNames().foreach(println)
    metaDB.close()
    chainDB.close()
  }
  def main(args: Array[String]): Unit = {
    val dbPath = "/Users/gaolianxin/Desktop/coding/20221201_tudb/TuDB/testdata"
    coldImportFromSpark(dbPath, "ethereum")
  }
}
