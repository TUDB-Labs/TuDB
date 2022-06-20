package org.grapheco.tudb.store.index

import org.grapheco.tudb.exception.TuDBException
import org.grapheco.tudb.store.storage.RocksDBStorage
import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompactionStyle, CompressionType, LRUCache, Options, RocksDB, WriteOptions}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable

/**
 * RocksIndexAPI
 */
class RocksIndexServerImpl(uri: String) extends IndexServer(uri) {

  private  var db: RocksDB= _

  val writeOptions=new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  def init(uri: String) = {
    logger.info(f"start Rocks db:${uri}")
    val dir = new File(uri)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException(
        "Invalid db path, it's a regular file: " + uri
      )

    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()
    tableConfig
      .setFilterPolicy(new BloomFilter(15, false))
      .setBlockSize(64L * 1024L)
      .setBlockCache(new LRUCache(1024L * 1024L * 1024L))


    options
      .setTableFormatConfig(tableConfig)
      .setCreateIfMissing(true)
      .setCompressionType(CompressionType.NO_COMPRESSION)
      .setCompactionStyle(CompactionStyle.NONE)
      .setDisableAutoCompactions(
        false
      ) // true will invalid the compaction trigger, maybe
      .setOptimizeFiltersForHits(
        false
      ) // true will not generate BloomFilter for L0.
      .setMaxBackgroundJobs(4)
      .setMaxWriteBufferNumber(8)
      .setSkipCheckingSstFileSizesOnDbOpen(true)
      .setLevelCompactionDynamicLevelBytes(true)
      .setAllowConcurrentMemtableWrite(true)
      .setCompactionReadaheadSize(2 * 1024 * 1024L)
      .setMaxOpenFiles(-1)
      .setUseDirectIoForFlushAndCompaction(true) // maybe importer use
      .setWriteBufferSize(256L * 1024L * 1024L)
      .setMinWriteBufferNumberToMerge(2) // 2 or 3 better performance
      .setLevel0FileNumCompactionTrigger(10) // level0 file num = 10, compression l0->l1 start.(invalid, why???);level0 size=256M * 3 * 10 = 7G
      .setLevel0SlowdownWritesTrigger(20)
      .setLevel0StopWritesTrigger(40)
      .setMaxBytesForLevelBase(
        256L * 1024L * 1024L * 2 * 10L
      ) // total size of level1(same as level0)
      .setMaxBytesForLevelMultiplier(10)
      .setTargetFileSizeBase(
        256L * 1024L * 1024L
      ) // maxBytesForLevelBase / 10 or 15
      .setTargetFileSizeMultiplier(2)


    try {
      db = RocksDB.open(options, uri)
    } catch {
      case ex: Exception =>
        throw new TuDBException(s"$uri, ${ex.getMessage}")
    }
  }

  private def objectToBytes(value: Any) = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(value)
    oos.flush()
    bos.toByteArray
  }

  private def bytesToObject(data: Array[Byte]) = {
    val oos = new ObjectInputStream(new ByteArrayInputStream(data))
    oos.readObject()
  }


  private def getDataFromDb(key: String) = {
    val keyBytes = objectToBytes(key)
    val dbValue = db.get(keyBytes)
    (keyBytes, dbValue)
  }

  def addIndex(key: String, value: Long): Unit = {
    val (keyBytes, dbValue) = getDataFromDb(key)
    val changeSet = if (dbValue == null) {
      new mutable.HashSet[Long]()
    } else {
      val obj = bytesToObject(dbValue)
      if (obj.isInstanceOf[mutable.Set[Long]]) obj.asInstanceOf[mutable.Set[Long]] else {
        logger.error(f"error index data key:${key} value type:${obj.toString}")
        new mutable.HashSet[Long]()
      }
    }
    changeSet.add(value)
    db.put(writeOptions,keyBytes,objectToBytes(changeSet))
  }

  def removeIndex(key: String, value: Long): Unit = {
    val (keyBytes, dbValue) = getDataFromDb(key)
    if (dbValue != null) {
      val obj = bytesToObject(dbValue)
      if (obj.isInstanceOf[mutable.Set[Long]]) {
        val changeData = obj.asInstanceOf[mutable.Set[Long]]
        changeData.remove(value)
        db.put(writeOptions,keyBytes, objectToBytes(changeData))
      }
    }
  }

  def getIndexByKey(key: String): Set[Long] = {
    val (keyBytes, dbValue) = getDataFromDb(key)
    if (dbValue != null) {
      val obj = bytesToObject(dbValue)
      if (obj.isInstanceOf[mutable.Set[Long]]) {
        obj.asInstanceOf[mutable.Set[Long]].toSet
      } else Set[Long]()
    } else Set[Long]()
  }

  def hasIndex(): Boolean = true


  override def close(): Unit = {
    db.close()
  }
}
