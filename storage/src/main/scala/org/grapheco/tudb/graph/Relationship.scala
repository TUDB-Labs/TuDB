package org.grapheco.tudb.graph

import org.grapheco.tudb.serializer.{BaseSerializer, RelationshipSerializer}

import collection.mutable.Map
import scala.collection.immutable.{Map => IMutableMap}

object Relationship {
  def loads(bytes: Array[Byte]): Relationship = {
    val allocator = RelationshipSerializer.allocator
    val byteBuffer = allocator.directBuffer()
    byteBuffer.writeBytes(bytes)
    val relationshipId: Long = byteBuffer.readLong()
    val fromID: Long = byteBuffer.readLong()
    val toID: Long = byteBuffer.readLong()
    val typeID: Int = byteBuffer.readInt()
    val properties: IMutableMap[Int, Any] = BaseSerializer.decodePropMap(byteBuffer)
    byteBuffer.release()
    new Relationship(relationshipId, fromID, toID, typeID, Map(properties.toSeq: _*))
  }
}

class Relationship(id: Long, from: Long, to: Long, typeId: Int, properties: Map[Int, Any]) {

  def addProperty(key: Int, value: Any) = {
    properties(key) = value
  }

  def property(key: Int): Option[Any] = {
    properties.get(key)
  }

  // FIXME: refactor to a general interface such as serializable
  def dumps(): Array[Byte] = {
    RelationshipSerializer.encodeRelationship(id, from, to, typeId, properties.toMap)
  }

  override def toString: String =
    s"<relationId: $id, relationType:$typeId, properties:{${properties.toList.mkString(",")}}>"
}
