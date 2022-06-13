package org.grapheco.tudb.store.relationship

import org.grapheco.tudb.serializer.{BaseSerializer, RelationshipSerializer}
import org.grapheco.tudb.store.relationship.RelationDirection.{Direction, IN}
import org.grapheco.tudb.store.storage.KeyValueDB

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 8:22 下午 2022/2/2
  * @Modified By:
  */
object RelationDirection extends Enumeration {
  type Direction = Value
  val IN = Value(0)
  val OUT = Value(1)
}
class RelationshipDirectionStore(db: KeyValueDB, DIRECTION: Direction) {

  /** in edge data structure
    * ------------------------
    * type(1Byte),nodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),fromNodeId(8Bytes)-->relationValue(id, properties)
    * ------------------------
    */

  def getKey(relation: StoredRelationship): Array[Byte] =
    if (DIRECTION == IN)
      RelationshipSerializer.encodeDirectedRelationshipKey(
        relation.to,
        relation.typeId,
        relation.from
      )
    else
      RelationshipSerializer.encodeDirectedRelationshipKey(
        relation.from,
        relation.typeId,
        relation.to
      )

  def set(relation: StoredRelationship): Unit = {
    val keyBytes = getKey(relation)
    db.put(keyBytes, BaseSerializer.encodeLong(relation.id))
  }

  def delete(relation: StoredRelationship): Unit = {
    val keyBytes = getKey(relation)
    db.delete(keyBytes)
  }

  def deleteRange(firstId: Long): Unit = {
    db.deleteRange(
      RelationshipSerializer.encodeRelationshipPrefix(firstId, 0),
      RelationshipSerializer.encodeRelationshipPrefix(firstId, -1)
    )
  }

  def deleteRange(firstId: Long, typeId: Int): Unit = {
    db.deleteRange(
      RelationshipSerializer.encodeDirectedRelationshipKey(firstId, typeId, 0),
      RelationshipSerializer.encodeDirectedRelationshipKey(firstId, typeId, -1)
    )
  }

  def get(node1: Long, edgeType: Int, node2: Long): Option[Long] = {
    val keyBytes = RelationshipSerializer.encodeDirectedRelationshipKey(
      node1,
      edgeType,
      node2
    )
    val value = db.get(keyBytes)
    if (value != null)
      Some(BaseSerializer.decodeLong(value))
    else
      None
  }

  def getNodeIds(nodeId: Long): Iterator[Long] = {
    val prefix = RelationshipSerializer.encodeRelationshipPrefix(nodeId)
    new NodeIdIterator(db, prefix)
  }

  def getNodeIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix =
      RelationshipSerializer.encodeRelationshipPrefix(nodeId, edgeType)
    new NodeIdIterator(db, prefix)
  }

  class NodeIdIterator(db: KeyValueDB, prefix: Array[Byte])
      extends Iterator[Long] {
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean =
      iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val fromNodeId = BaseSerializer.decodeLong(iter.key(), 12)
      iter.next()
      fromNodeId
    }
  }

  def getRelationIds(nodeId: Long): Iterator[Long] = {
    val prefix = RelationshipSerializer.encodeRelationshipPrefix(nodeId)
    new RelationIdIterator(db, prefix)
  }

  def getRelationIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix =
      RelationshipSerializer.encodeRelationshipPrefix(nodeId, edgeType)
    new RelationIdIterator(db, prefix)
  }

  class RelationIdIterator(db: KeyValueDB, prefix: Array[Byte])
      extends Iterator[Long] {
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean =
      iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val id = BaseSerializer.decodeLong(iter.value())
      iter.next()
      id
    }
  }

  def getRelations(nodeId: Long): Iterator[StoredRelationship] = {
    val prefix = RelationshipSerializer.encodeRelationshipPrefix(nodeId)
    new RelationshipIterator(db, prefix)
  }

  def getRelations(
      nodeId: Long,
      edgeType: Int
  ): Iterator[StoredRelationship] = {
    val prefix =
      RelationshipSerializer.encodeRelationshipPrefix(nodeId, edgeType)
    new RelationshipIterator(db, prefix)
  }

  def getRelation(
      firstNodeId: Long,
      edgeType: Int,
      secondNodeId: Long
  ): Option[StoredRelationship] = {
    val key = RelationshipSerializer.encodeDirectedRelationshipKey(
      firstNodeId,
      edgeType,
      secondNodeId
    )
    val values = db.get(key)
    if (values == null) None
    else {
      val id = BaseSerializer.decodeLong(values)
      if (DIRECTION == IN)
        Option(StoredRelationship(id, secondNodeId, firstNodeId, edgeType))
      else Option(StoredRelationship(id, firstNodeId, secondNodeId, edgeType))
    }
  }

  class RelationshipIterator(db: KeyValueDB, prefix: Array[Byte])
      extends Iterator[StoredRelationship] {
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean =
      iter.isValid && iter.key().startsWith(prefix)

    override def next(): StoredRelationship = {
      val key = iter.key()
      val decodedKey =
        RelationshipSerializer.decodeRelationshipDirectionKey(key)
      val node1 = decodedKey._1
      val typeId = decodedKey._2
      val node2 = decodedKey._3
      val relationshipId = BaseSerializer.decodeLong(iter.value())

      val res =
        if (DIRECTION == IN)
          StoredRelationship(relationshipId, node2, node1, typeId)
        else StoredRelationship(relationshipId, node1, node2, typeId)
      iter.next()
      res
    }
  }

  def all(): Iterator[StoredRelationship] = {
    new RelationshipIterator(db, Array.emptyByteArray)
  }

  def close(): Unit = {
    db.close()
  }
}
