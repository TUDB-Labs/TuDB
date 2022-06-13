package org.grapheco.tudb.store.relationship

import org.grapheco.tudb.store.meta.{DBNameMap, IdGenerator, PropertyNameStore, RelationshipTypeNameStore}
import org.grapheco.tudb.store.storage.{KeyValueDB, RocksDBStorage}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 9:27 下午 2022/2/2
  * @Modified By:
  */
class RelationshipStoreAPI(
    relationDBPath: String,
    relationConfigPath: String,
    inRelationDBPath: String,
    inRelationConfigPath: String,
    outRelationDBPath: String,
    outRelationConfigPath: String,
    relationLabelDBPath: String,
    relationLabelConfigPath: String,
    metaDB: KeyValueDB
) extends RelationStoreSPI {

  private val relationDB =
    RocksDBStorage.getDB(relationDBPath, rocksdbConfigPath = relationConfigPath)
  private val inRelationDB = RocksDBStorage.getDB(
    inRelationDBPath,
    rocksdbConfigPath = inRelationConfigPath
  )
  private val outRelationDB = RocksDBStorage.getDB(
    outRelationDBPath,
    rocksdbConfigPath = outRelationConfigPath
  )
  private val relationLabelDB = RocksDBStorage.getDB(
    relationLabelDBPath,
    rocksdbConfigPath = relationLabelConfigPath
  )

  private val relationStore = new RelationshipPropertyStore(relationDB)
  private val inRelationStore =
    new RelationshipDirectionStore(inRelationDB, RelationDirection.IN)

  private val outRelationStore =
    new RelationshipDirectionStore(outRelationDB, RelationDirection.OUT)

  private val relationLabelStore = new RelationshipLabelIndex(relationLabelDB)
  private val relationTypeNameStore = new RelationshipTypeNameStore(metaDB)
  private val propertyName = new PropertyNameStore(metaDB)

  private val relationIdGenerator = new IdGenerator(relationDB, 200)

  def this(
      dbPath: String,
      rocksdbCfgPath: String = "default",
      metaDB: KeyValueDB
  ) {
    this(
      s"${dbPath}/${DBNameMap.relationDB}",
      rocksdbCfgPath,
      s"${dbPath}/${DBNameMap.inRelationDB}",
      rocksdbCfgPath,
      s"${dbPath}/${DBNameMap.outRelationDB}",
      rocksdbCfgPath,
      s"${dbPath}/${DBNameMap.relationLabelDB}",
      rocksdbCfgPath,
      metaDB
    )
  }

  override def allRelationTypes(): Array[String] =
    relationTypeNameStore.mapString2Int.keys.toArray

  override def allRelationTypeIds(): Array[Int] =
    relationTypeNameStore.mapInt2String.keys.toArray

  override def relationCount: Long = relationStore.count

  override def getRelationTypeName(relationTypeId: Int): Option[String] =
    relationTypeNameStore.key(relationTypeId)

  override def getRelationTypeId(relationTypeName: String): Option[Int] =
    relationTypeNameStore.id(relationTypeName)

  override def addRelationType(relationTypeName: String): Int =
    relationTypeNameStore.getOrAddId(relationTypeName)

  override def allPropertyKeys(): Array[String] =
    propertyName.mapString2Int.keys.toArray

  override def allPropertyKeyIds(): Array[Int] =
    propertyName.mapInt2String.keys.toArray

  override def getPropertyKeyName(keyId: Int): Option[String] =
    propertyName.key(keyId)

  override def getPropertyKeyId(keyName: String): Option[Int] =
    propertyName.id(keyName)

  override def addPropertyKey(keyName: String): Int =
    propertyName.getOrAddId(keyName)

  override def getRelationById(
      relId: Long
  ): Option[StoredRelationshipWithProperty] = relationStore.get(relId)

  override def getRelationIdsByRelationType(
      relationTypeId: Int
  ): Iterator[Long] =
    relationLabelStore.getRelations(relationTypeId)

  //TODO: Enable to set multi props on once invoke.
  override def relationSetProperty(
      relationId: Long,
      propertyKeyId: Int,
      propertyValue: Any
  ): Unit = {
    relationStore.get(relationId).foreach { rel =>
      relationStore.set(
        rel.id,
        rel.from,
        rel.to,
        rel.typeId,
        rel.properties ++ Map(propertyKeyId -> propertyValue)
      )
    }
  }

  override def relationRemoveProperty(
      relationId: Long,
      propertyKeyId: Int
  ): Any = {
    relationStore.get(relationId).foreach { rel =>
      relationStore.set(
        rel.id,
        rel.from,
        rel.to,
        rel.typeId,
        rel.properties - propertyKeyId
      )
    }
  }

  override def findToNodeIds(fromNodeId: Long): Iterator[Long] =
    outRelationStore.getNodeIds(fromNodeId)

  override def findToNodeIds(
      fromNodeId: Long,
      relationType: Int
  ): Iterator[Long] =
    outRelationStore.getNodeIds(fromNodeId, relationType)

  override def findFromNodeIds(toNodeId: Long): Iterator[Long] =
    inRelationStore.getNodeIds(toNodeId)

  override def findFromNodeIds(
      toNodeId: Long,
      relationType: Int
  ): Iterator[Long] =
    inRelationStore.getNodeIds(toNodeId, relationType)

  override def addRelation(relation: StoredRelationship): Unit = {
    relationStore.set(relation)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
    relationLabelStore.set(relation.typeId, relation.id)
  }

  override def addRelation(relation: StoredRelationshipWithProperty): Unit = {
    relationStore.set(relation)
    inRelationStore.set(relation)
    outRelationStore.set(relation)
    relationLabelStore.set(relation.typeId, relation.id)
  }

  override def addRelationship(
      relationshipId: Long,
      fromId: Long,
      toId: Long,
      typeId: Int,
      props: Map[Int, Any]
  ): Unit = {
    addRelation(
      new StoredRelationshipWithProperty(
        relationshipId,
        fromId,
        toId,
        typeId,
        props
      )
    )
  }

  override def deleteRelation(id: Long): Unit = {
    relationStore.get(id).foreach { relation =>
      relationStore.delete(id)
      inRelationStore.delete(relation)
      outRelationStore.delete(relation)
      relationLabelStore.delete(relation.typeId, relation.id)
    }
  }

  override def allRelations(
      withProperty: Boolean = false
  ): Iterator[StoredRelationship] = {
    if (withProperty) relationStore.all()
    else inRelationStore.all()
  }

  override def findOutRelations(
      fromNodeId: Long,
      edgeType: Option[Int] = None
  ): Iterator[StoredRelationship] =
    edgeType
      .map(outRelationStore.getRelations(fromNodeId, _))
      .getOrElse(outRelationStore.getRelations(fromNodeId))

  override def findInRelations(
      toNodeId: Long,
      edgeType: Option[Int] = None
  ): Iterator[StoredRelationship] =
    edgeType
      .map(inRelationStore.getRelations(toNodeId, _))
      .getOrElse(inRelationStore.getRelations(toNodeId))

  override def findInRelationsBetween(
      toNodeId: Long,
      fromNodeId: Long,
      edgeType: Option[Int] = None
  ): Iterator[StoredRelationship] = {
    edgeType
      .map(inRelationStore.getRelation(toNodeId, _, fromNodeId).toIterator)
      .getOrElse(
        inRelationStore.getRelations(toNodeId).filter(r => r.from == fromNodeId)
      )
  }

  override def findOutRelationsBetween(
      fromNodeId: Long,
      toNodeId: Long,
      edgeType: Option[Int] = None
  ): Iterator[StoredRelationship] = {
    edgeType
      .map(outRelationStore.getRelation(fromNodeId, _, toNodeId).toIterator)
      .getOrElse(
        outRelationStore.getRelations(fromNodeId).filter(r => r.to == toNodeId)
      )
  }

  override def close(): Unit = {
    relationStore.close()
    inRelationStore.close()
    outRelationDB.close()
    metaDB.close()
    relationLabelStore.close()
  }

  override def newRelationId(): Long = {
    relationIdGenerator.nextId()
  }
}
