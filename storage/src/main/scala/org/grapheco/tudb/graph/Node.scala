package org.grapheco.tudb.graph

import org.grapheco.tudb.serializer.{BaseSerializer, NodeSerializer, SerializerDataType}
import scala.collection.immutable.{Map => IMutableMap}
import scala.collection.mutable.Map
import scala.collection.mutable.Set

object Node {
  def loads(bytes: Array[Byte]): Node = {
    val allocator = NodeSerializer.allocator
    val byteBuffer = allocator.directBuffer()
    byteBuffer.writeBytes(bytes)
    val id: Long = byteBuffer.readLong()
    val labelIDsTypeFlag = SerializerDataType(byteBuffer.readByte().toInt)
    val labelIDs: Array[Int] = BaseSerializer
      .decodeArray(byteBuffer, labelIDsTypeFlag)
      .asInstanceOf[Array[Int]]
    val props: IMutableMap[Int, Any] = BaseSerializer.decodePropMap(byteBuffer)
    byteBuffer.release()
    new Node(id, Set(labelIDs: _*), Map(props.toSeq: _*))
  }
}

class Node(id: Long, labelIds: Set[Int], properties: Map[Int, Any]) {

  def addLabelId(labelId: Int): Unit = {
    labelIds.add(labelId)
  }

  def addProperty(key: Int, value: Any): Unit = {
    properties(key) = value
  }

  def property(key: Int): Option[Any] = {
    // Raise error if not existed
    properties.get(key)
  }

  // FIXME: refactor to a general interface such as serializable
  def dumps(): Array[Byte] = {
    NodeSerializer.encodeNodeWithProperties(id, labelIds.toArray, properties.toMap)
  }

  override def toString: String =
    s"<nodeId: $id, labels:[${labelIds.mkString(",")}], properties:{${properties.toList.mkString(",")}}>"
}
