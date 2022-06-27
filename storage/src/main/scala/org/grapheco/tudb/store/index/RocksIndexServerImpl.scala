/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.store.index

import org.grapheco.tudb.exception.TuDBException
import org.grapheco.tudb.store.storage.RocksDBStorage
import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompactionStyle, CompressionType, LRUCache, Options, RocksDB, WriteOptions}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable

/** @author: huagnlin
 * @createDate: 2022-06-20 17:19:08
 * @description: this is the rocksdb index engine . data storage on file
 */
class RocksIndexServerImpl(params: Map[String, String]) extends IndexServer(params) {

  private var db: RocksDB = _

  val writeOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  /**
   * uri is rocksdb data storage location
   *
   * @see [[IndexServer.init()]]
   */
  def init(params: Map[String, String]) = {
    logger.info(f"start Rocks db:${params}")
    val path = params.getOrElse("path", null)
    if (path == null) {
      throw new TuDBException(f"Rocksdb path is null")
    }
    // make directory
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException(
        "Invalid db path, it's a regular file: " + path
      )
    // optimization rocksdb params
    val options: Options = new Options()
    val tableConfig = new BlockBasedTableConfig()
    tableConfig
      .setFilterPolicy(new BloomFilter(15, false))
      .setBlockSize(64L * 1024L)
      .setBlockCache(new LRUCache(1024L * 1024L * 1024L))

    //TODO:  configure  parameters
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
    // create rocksdb instance
    try {
      db = RocksDB.open(options, path)
    } catch {
      case ex: Exception =>
        throw new TuDBException(s"$path, ${ex.getMessage}")
    }
  }

  /**
   * convert object to bytes
   *
   * @param any object
   * @return byte arrays
   */
  private def objectToBytes(value: Any) = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(value)
    oos.flush()
    baos.toByteArray
  }

  /**
   * convert bytes to object
   *
   * @param byte array
   * @return object
   */
  private def bytesToObject(data: Array[Byte]) = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(data))
    ois.readObject()
  }

  /**
   * get data from rocksdb
   *
   * @param key
   * @return (byte array key ,byte array value from db )
   */
  private def getDataFromDB(key: String) = {
    val keyBytes = objectToBytes(key)
    val dbValue = db.get(keyBytes)
    (keyBytes, dbValue)
  }

  /**
   * @see [[IndexServer.addIndex()]]
   */
  def addIndex(key: String, value: Long): Unit = {
    val (keyBytes, dbValue) = getDataFromDB(key)
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
    db.put(writeOptions, keyBytes, objectToBytes(changeSet))
  }

  /**
   * @see [[IndexServer.removeIndex()]]
   */
  def removeIndex(key: String, value: Long): Unit = {
    val (keyBytes, dbValue) = getDataFromDB(key)
    if (dbValue != null) {
      val obj = bytesToObject(dbValue)
      if (obj.isInstanceOf[mutable.Set[Long]]) {
        val changeData = obj.asInstanceOf[mutable.Set[Long]]
        changeData.remove(value)
        db.put(writeOptions, keyBytes, objectToBytes(changeData))
      }
    }
  }

  /**
   * @see [[IndexServer.getIndexByKey()]]
   */
  def getIndexByKey(key: String): Set[Long] = {
    val (_, dbValue) = getDataFromDB(key)
    if (dbValue != null) {
      val obj = bytesToObject(dbValue)
      if (obj.isInstanceOf[mutable.Set[Long]]) {
        obj.asInstanceOf[mutable.Set[Long]].toSet
      } else Set[Long]()
    } else Set[Long]()
  }

  /**
   * @see [[IndexServer.hasIndex()]]
   */
  def hasIndex(): Boolean = true

  /**
   * @see [[IndexServer.close()]]
   */
  override def close(): Unit = {
    db.close()
  }
}
