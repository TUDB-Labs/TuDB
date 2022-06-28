package org.grapheco.tudb.store.storage

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.tudb.exception.TuDBException
import org.rocksdb._

import java.io.File

object RocksDBStorage extends LazyLogging {
  RocksDB.loadLibrary()

  def getDB(
      path: String,
      createIfMissing: Boolean = true,
      withBloomFilter: Boolean = true,
      isHDD: Boolean = false,
      useForImporter: Boolean = false,
      prefix: Int = 0,
      rocksdbConfigPath: String = "default"
  ): KeyValueDB = {

    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
    if (dir.exists && !dir.isDirectory)
      throw new IllegalStateException(
        "Invalid db path, it's a regular file: " + path
      )

    if (rocksdbConfigPath == "default") {
      logger.debug("use default setting")

      try {
        new RocksDBStorage(RocksDB.open(RocksDBStorageConfig.getDefault(createIfMissing), path))
      } catch {
        case ex: Exception =>
          throw new TuDBException(s"$path, ${ex.getMessage}")
      }

    } else if (rocksdbConfigPath == "performance") {
      logger.debug("use performance setting")

      try {
        new RocksDBStorage(RocksDB.open(RocksDBStorageConfig.getPerformance(createIfMissing), path))
      } catch {
        case ex: Exception =>
          throw new TuDBException(s"$path, ${ex.getMessage}")
      }

    } else {
      logger.debug("read setting file")
      val rocksFile = new File(rocksdbConfigPath)
      if (!rocksFile.exists())
        throw new TuDBException("rocksdb config file not exist...")
      val options = new RocksDBConfigBuilder(rocksFile).getOptions()
      val db = RocksDB.open(options, path)
      new RocksDBStorage(db)
    }
  }
}

class RocksDBStorage(val rocksDB: RocksDB) extends KeyValueDB {

  override def get(key: Array[Byte]): Array[Byte] = {
    rocksDB.get(key)
  }

  override def put(key: Array[Byte], value: Array[Byte]): Unit =
    rocksDB.put(key, value)

  override def write(option: Any, batch: Any): Unit = rocksDB.write(
    option.asInstanceOf[WriteOptions],
    batch.asInstanceOf[WriteBatch]
  )

  override def delete(key: Array[Byte]): Unit = rocksDB.delete(key)

  override def deleteRange(key1: Array[Byte], key2: Array[Byte]): Unit =
    rocksDB.deleteRange(key1, key2)

  override def newIterator(): KeyValueIterator = {
    val iter = rocksDB.newIterator()
    new KeyValueIterator {
      override def isValid: Boolean = iter.isValid

      override def seek(key: Array[Byte]): Unit = iter.seek(key)

      override def seekToFirst(): Unit = iter.seekToFirst()

      override def seekToLast(): Unit = iter.seekToLast()

      override def seekForPrev(key: Array[Byte]): Unit = iter.seekForPrev(key)

      override def next(): Unit = iter.next()

      override def prev(): Unit = iter.prev()

      override def key(): Array[Byte] = iter.key()

      override def value(): Array[Byte] = iter.value()
    }
  }

  override def flush(): Unit = rocksDB.flush(new FlushOptions)

  override def close(): Unit = rocksDB.close()
}
