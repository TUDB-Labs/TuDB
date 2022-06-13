package org.grapheco.tudb.importer

import org.grapheco.tudb.serializer.{BaseSerializer, KeyConverter}
import org.grapheco.tudb.store.meta.DBNameMap
import org.grapheco.tudb.store.storage.RocksDBStorage

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

object PDBMetaData {

  private val _nodeIdAllocator: AtomicLong = new AtomicLong(0)
  private val _relationIdAllocator: AtomicLong = new AtomicLong(0)
  private val _indexIdAllocator: AtomicInteger = new AtomicInteger(0)

  def availableNodeId: Long = _nodeIdAllocator.getAndIncrement()
  def availableRelId: Long = _relationIdAllocator.getAndIncrement()
  def availabelIndexId: Int = _indexIdAllocator.getAndIncrement()

  private val _propIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)
  private val _typeIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)
  private val _labelIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)

  def persist(dbPath: String): Unit = {
    val rocksDB = RocksDBStorage.getDB(s"${dbPath}/metadata")
    rocksDB.put(
      "_nodeIdAllocator".getBytes(),
      BaseSerializer.encodeLong(_nodeIdAllocator.get())
    )
    rocksDB.put(
      "_relationIdAllocator".getBytes(),
      BaseSerializer.encodeLong(_relationIdAllocator.get())
    )
    rocksDB.put(
      "_indexIdAllocator".getBytes(),
      BaseSerializer.encodeLong(_indexIdAllocator.get())
    )
    rocksDB.put("_propIdManager".getBytes(), _propIdManager.serialized)
    rocksDB.put("_typeIdManager".getBytes(), _typeIdManager.serialized)
    rocksDB.put("_labelIdManager".getBytes(), _labelIdManager.serialized)

    val nodeMetaDB = RocksDBStorage.getDB(s"${dbPath}/${DBNameMap.nodeMetaDB}")
    val relMetaDB =
      RocksDBStorage.getDB(s"${dbPath}/${DBNameMap.relationMetaDB}")
    nodeMetaDB.put(
      KeyConverter.nodeIdGeneratorKeyToBytes(),
      BaseSerializer.encodeLong(_nodeIdAllocator.get())
    )
    relMetaDB.put(
      KeyConverter.relationIdGeneratorKeyToBytes(),
      BaseSerializer.encodeLong(_relationIdAllocator.get())
    )
    _labelIdManager.all.foreach { kv =>
      val key = KeyConverter.nodeLabelKeyToBytes(kv._1)
      nodeMetaDB.put(key, BaseSerializer.encodeString(kv._2))
    }
    _typeIdManager.all.foreach { kv =>
      val key = KeyConverter.relationTypeKeyToBytes(kv._1)
      relMetaDB.put(key, BaseSerializer.encodeString(kv._2))
    }
    _propIdManager.all.foreach { kv =>
      val key = KeyConverter.propertyNameKeyToBytes(kv._1)
      nodeMetaDB.put(key, BaseSerializer.encodeString(kv._2))
      relMetaDB.put(key, BaseSerializer.encodeString(kv._2))
    }
    nodeMetaDB.flush()
    nodeMetaDB.close()
    relMetaDB.flush()
    relMetaDB.close()
    rocksDB.flush()
    rocksDB.close()
  }

  def init(dbPath: String): Unit = {
    val rocksDB = RocksDBStorage.getDB(s"${dbPath}/metadata")
    _nodeIdAllocator.set(
      BaseSerializer.decodeLong(rocksDB.get("_nodeIdAllocator".getBytes()))
    )
    _relationIdAllocator.set(
      BaseSerializer.decodeLong(rocksDB.get("_relationIdAllocator".getBytes()))
    )
    _indexIdAllocator.set(
      BaseSerializer.decodeInt(rocksDB.get("_indexIdAllocator".getBytes()))
    )
    _propIdManager.init(rocksDB.get("_propIdManager".getBytes()))
    _typeIdManager.init(rocksDB.get("_typeIdManager".getBytes()))
    _labelIdManager.init(rocksDB.get("_labelIdManager".getBytes()))
    rocksDB.close()
  }

  def isPropExists(prop: String): Boolean = _propIdManager.isNameExists(prop)

  def isLabelExists(label: String): Boolean =
    _labelIdManager.isNameExists(label)

  def isTypeExists(edgeType: String): Boolean =
    _typeIdManager.isNameExists(edgeType)

  def getPropId(prop: String): Int = {
    _propIdManager.getId(prop)
  }

  def getPropName(propId: Int): String = {
    _propIdManager.getName(propId)
  }

  def getLabelId(label: String): Int = {
    _labelIdManager.getId(label)
  }

  def getLabelName(labelId: Int): String = {
    _labelIdManager.getName(labelId)
  }

  def getTypeId(edgeType: String): Int = {
    _typeIdManager.getId(edgeType)
  }

  def getTypeName(typeId: Int): String = {
    _typeIdManager.getName(typeId)
  }
}
