/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.store.index

import org.grapheco.tudb.exception.TuDBException
import org.grapheco.tudb.store.storage.{KeyValueDB, RocksDBStorage}
import org.rocksdb.{BlockBasedTableConfig, BloomFilter, CompactionStyle, CompressionType, LRUCache, Options, RocksDB, WriteOptions}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable

/** @author: huagnlin
 * @createDate: 2022-06-20 17:19:08
 * @description: this is the rocksdb index engine . data storage on file
 */
class RocksIndexServerImpl(params: Map[String, String]) extends IndexServer(params) {

  private var db: KeyValueDB = _

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
    db = RocksDBStorage.getDB(path,rocksdbConfigPath="performance")
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
    db.put(keyBytes, objectToBytes(changeSet))
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
        db.put(keyBytes, objectToBytes(changeData))
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
