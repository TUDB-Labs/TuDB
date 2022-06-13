package org.grapheco.tudb.serializer

import io.netty.buffer.PooledByteBufAllocator
import org.grapheco.tudb.store.meta.TypeManager.{KeyType, NodeId, RelationshipId, TypeId}
import org.grapheco.tudb.store.relationship.{StoredRelationship, StoredRelationshipWithProperty}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 8:49 下午 2022/1/26
  * @Modified By:
  */
object RelationshipSerializer extends AbstractRelationshipSerializer {
  val allocator: PooledByteBufAllocator = PooledByteBufAllocator.DEFAULT

  override def encodeRelationship(
      relationId: Long,
      fromId: Long,
      toId: Long,
      typeId: Int,
      props: Map[Int, Any]
  ): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeLong(relationId)
    _bytebuf.writeLong(fromId)
    _bytebuf.writeLong(toId)
    _bytebuf.writeInt(typeId)
    BaseSerializer.encodePropMap(props, _bytebuf)
    val bytes = BaseSerializer.releaseBuf(_bytebuf)
    bytes
  }

  override def encodeRelationship(
      storedRelationship: StoredRelationship
  ): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeLong(storedRelationship.id)
    _bytebuf.writeLong(storedRelationship.from)
    _bytebuf.writeLong(storedRelationship.to)
    _bytebuf.writeInt(storedRelationship.typeId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  override def decodeRelationshipWithProperties(
      bytes: Array[Byte]
  ): StoredRelationshipWithProperty = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val relationshipId: Long = _bytebuf.readLong()
    val fromID: Long = _bytebuf.readLong()
    val toID: Long = _bytebuf.readLong()
    val typeID: Int = _bytebuf.readInt()
    _bytebuf.release()
    new StoredRelationshipWithProperty(
      relationshipId,
      fromID,
      toID,
      typeID,
      bytes
    )
  }

  // [relationId, fromId, toID, typeId, props]
  override def decodePropertiesFromFullRelationship(
      bytes: Array[Byte]
  ): Map[Int, Any] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    _bytebuf.readerIndex(28)
    val properties: Map[Int, Any] = BaseSerializer.decodePropMap(_bytebuf)
    _bytebuf.release()
    properties
  }

  def encodeRelationshipKey(relationshipId: RelationshipId): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeLong(relationshipId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def encodeRelationshipKey(
      typeId: TypeId,
      relationshipId: RelationshipId
  ): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeInt(typeId)
    _bytebuf.writeLong(relationshipId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def encodeDirectedRelationshipKey(
      fromId: NodeId,
      typeId: TypeId,
      toId: NodeId
  ): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeLong(fromId)
    _bytebuf.writeInt(typeId)
    _bytebuf.writeLong(toId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  // [fromNodeId(8Bytes),----,----]
  def encodeRelationshipPrefix(fromNodeId: NodeId): Array[Byte] = {
    BaseSerializer.encodeLong(fromNodeId)
  }

  // [fromNodeId(8Bytes),relationType(4Bytes),----]
  def encodeRelationshipPrefix(
      fromNodeId: NodeId,
      typeId: TypeId
  ): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeLong(fromNodeId)
    _bytebuf.writeInt(typeId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def decodeRelationshipDirectionKey(
      bytes: Array[Byte]
  ): (NodeId, TypeId, NodeId) = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val node1Id = _bytebuf.readLong()
    val typeId = _bytebuf.readInt()
    val node2Id = _bytebuf.readLong()
    _bytebuf.release()
    (node1Id, typeId, node2Id)
  }

  def encodeRelationshipTypeKey(
      typeId: TypeId,
      relationshipId: RelationshipId
  ): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeInt(typeId)
    _bytebuf.writeLong(relationshipId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def encodeRelationshipTypeKey(typeId: TypeId): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeInt(typeId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def decodeRelationshipTypeKey(
      bytes: Array[Byte]
  ): (TypeId, RelationshipId) = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val typeId = _bytebuf.readInt()
    val relationshipId = _bytebuf.readLong()
    _bytebuf.release()
    (typeId, relationshipId)
  }

  def encodeRelationshipTypeName(typeId: TypeId): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeByte(KeyType.RelationshipType.id.toByte)
    _bytebuf.writeInt(typeId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def relationshipTypeNameKeyPrefix(): Array[Byte] = Array(
    KeyType.RelationshipType.id.toByte
  )
}
