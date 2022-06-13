package org.grapheco.tudb.serializer
import io.netty.buffer.{ByteBuf, PooledByteBufAllocator}
import org.grapheco.tudb.store.meta.TypeManager.{LabelId, NodeId}
import org.grapheco.tudb.store.node.StoredNodeWithProperty

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 10:25 下午 2022/1/25
  * @Modified By:
  */
object NodeSerializer extends AbstractNodeSerializer {
  val allocator: PooledByteBufAllocator = PooledByteBufAllocator.DEFAULT

  override def encodeNodeKey(
      id: Long,
      labelIds: Array[Int]
  ): Array[Array[Byte]] = {
    val _bytebuf = allocator.directBuffer()
    val bytes: Array[Array[Byte]] = labelIds.map(labelId => {
      _bytebuf.writeInt(labelId)
      _bytebuf.writeLong(id)
      BaseSerializer.exportBuf(_bytebuf)
    })
    _bytebuf.release()
    bytes
  }

  override def encodeNodeKey(id: Long, labelId: Int): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeInt(labelId)
    _bytebuf.writeLong(id)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  override def encodeNodeLabels(labelIds: Array[LabelId]): Array[Byte] = {
    BaseSerializer.encodeArray(labelIds)
  }

  override def encodeNodeProperties(props: Map[Int, Any]): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    BaseSerializer.encodePropMap(props, _bytebuf)
    val bytes: Array[Byte] = BaseSerializer.releaseBuf(_bytebuf)
    bytes
  }

  override def encodeNodeWithProperties(
      storedNodeWithProperty: StoredNodeWithProperty
  ): Array[Byte] = {
    storedNodeWithProperty.sourceBytes
  }

  override def encodeNodeWithProperties(
      id: Long,
      labelIDs: Array[Int],
      properties: Map[Int, Any]
  ): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeLong(id)
    BaseSerializer.encodeArray(labelIDs, _bytebuf)
    BaseSerializer.encodePropMap(properties, _bytebuf)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  override def decodeNodeWithProperties(
      bytes: Array[Byte]
  ): StoredNodeWithProperty = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val id: Long = _bytebuf.readLong()
    val labelIDsTypeFlag = SerializerDataType(_bytebuf.readByte().toInt)
    val labelIDs: Array[Int] = BaseSerializer
      .decodeArray(_bytebuf, labelIDsTypeFlag)
      .asInstanceOf[Array[Int]]
    _bytebuf.release()
    new StoredNodeWithProperty(id, labelIDs, bytes)
  }

  def decodeNodeWithProperties(
      _byteBuf: ByteBuf,
      bytes: Array[Byte]
  ): StoredNodeWithProperty = {
    _byteBuf.writeBytes(bytes)
    val id: Long = _byteBuf.readLong()
    val labelIDsTypeFlag = SerializerDataType(_byteBuf.readByte().toInt)
    val labelIDs: Array[Int] = BaseSerializer
      .decodeArray(_byteBuf, labelIDsTypeFlag)
      .asInstanceOf[Array[Int]]
    _byteBuf.clear()
    new StoredNodeWithProperty(id, labelIDs, bytes)
  }

  override def decodePropertiesFromFullNode(
      bytes: Array[Byte]
  ): Map[Int, Any] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val id: Long = _bytebuf.readLong()
    val labelIDsTypeFlag = SerializerDataType(_bytebuf.readByte().toInt)
    val labelIDs: Array[Int] = BaseSerializer
      .decodeArray(_bytebuf, labelIDsTypeFlag)
      .asInstanceOf[Array[Int]]
    val props: Map[Int, Any] = BaseSerializer.decodePropMap(_bytebuf)
    _bytebuf.release()
    props
  }

  override def decodeNodeKey(bytes: Array[Byte]): (NodeId, LabelId) = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val labelId = _bytebuf.readInt()
    val nodeId = _bytebuf.readLong()
    _bytebuf.release()
    (nodeId, labelId)
  }

  def decodeNodeKey(
      _byteBuf: ByteBuf,
      bytes: Array[Byte]
  ): (NodeId, LabelId) = {
    _byteBuf.writeBytes(bytes)
    val labelId = _byteBuf.readInt()
    val nodeId = _byteBuf.readLong()
    _byteBuf.clear()
    (nodeId, labelId)
  }

  //NodeLabelKey: [nodeId, labelId]
  def encodeNodeLabelKey(nodeId: NodeId, labelId: LabelId): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeLong(nodeId)
    _bytebuf.writeInt(labelId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def encodeNodeLabelKey(nodeId: NodeId): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeLong(nodeId)
    // FIXME: [0000,00000001] --> [000000001,0000]
//    _bytebuf.writeInt(NONE_LABEL_ID)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def decodeNodeIdInNodeLabelKey(bytes: Array[Byte]): NodeId = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val nodeId = _bytebuf.readLong()
    _bytebuf.release()
    nodeId
  }

  def decodeLabelIdInNodeLabelKey(bytes: Array[Byte]): LabelId = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    _bytebuf.readLong()
    val labelId: Int =
      try {
        _bytebuf.readInt()
      } catch {
        case ex: java.lang.IndexOutOfBoundsException => SpecialID.NONE_LABEL_ID
      }
    _bytebuf.release()
    labelId
  }

  def decodeNodeLabelIds(bytes: Array[Byte]): Array[Int] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val propType = SerializerDataType(_bytebuf.readByte().toInt)
    val labelIds: Array[Int] =
      BaseSerializer.decodeArray(_bytebuf, propType).asInstanceOf[Array[Int]]
    _bytebuf.release()
    labelIds
  }

}
