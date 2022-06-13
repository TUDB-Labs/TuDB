package org.grapheco.tudb.store.node

import io.netty.buffer.PooledByteBufAllocator
import org.grapheco.tudb.serializer.SpecialID.NONE_LABEL_ID
import org.grapheco.tudb.serializer.{BaseSerializer, NodeSerializer}
import org.grapheco.tudb.store.meta.TypeManager._
import org.grapheco.tudb.store.storage.KeyValueDB

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 12:20 下午 2022/1/29
  * @Modified By:
  */
class NodeStore(db: KeyValueDB) {
  // The allocator is used to allocate ByteBuf for high performance Serializing.
  val allocator: PooledByteBufAllocator = PooledByteBufAllocator.DEFAULT

  // [labelId,nodeId]->[Node]
  def set(
      nodeId: NodeId,
      labelIds: Array[LabelId],
      value: Array[Byte]
  ): Unit = {
    if (labelIds.nonEmpty)
      labelIds.foreach(labelId =>
        db.put(NodeSerializer.encodeNodeKey(nodeId, labelId), value)
      )
    else
      db.put(NodeSerializer.encodeNodeKey(nodeId, NONE_LABEL_ID), value)
  }

  def set(labelId: LabelId, node: StoredNodeWithProperty): Unit =
    db.put(NodeSerializer.encodeNodeKey(node.id, labelId), node.sourceBytes)

  def set(node: StoredNodeWithProperty): Unit =
    set(node.id, node.labelIds, node.sourceBytes)

  def get(nodeId: NodeId, labelId: LabelId): Option[StoredNodeWithProperty] = {
    val value = db.get(NodeSerializer.encodeNodeKey(nodeId, labelId))
    if (value != null) Some(NodeSerializer.decodeNodeWithProperties(value))
    else None
  }

  def all(): Iterator[StoredNodeWithProperty] = {
    val iter = db.newIterator()
    iter.seekToFirst()

    val allNodes: Iterator[StoredNodeWithProperty] =
      new Iterator[StoredNodeWithProperty]() {
        // Fixme: Release the two byteBuf properly.
        // Warning: Release them after the iter was destoried.
        // These byteBufs may cause LEAK warning.
        // But it would not crash the JVM.
        private val _byteBuf1 = allocator.directBuffer()
        private val _byteBuf2 = allocator.directBuffer()
        override def hasNext: Boolean = {
          if (iter.isValid) {
            true
          } else {
            _byteBuf1.release()
            _byteBuf2.release()
            false
          }
        }
        override def next(): StoredNodeWithProperty = {
          val node =
            NodeSerializer.decodeNodeWithProperties(_byteBuf1, iter.value())
          val label = NodeSerializer.decodeNodeKey(_byteBuf2, iter.key())._2
          iter.next()
          if (node.labelIds.length > 0 && node.labelIds(0) != label) null
          else node
        }
      }.filter(_ != null)

    allNodes
  }

  def getNodesByLabel(labelId: LabelId): Iterator[StoredNodeWithProperty] = {
    val iter = db.newIterator()
    val prefix = BaseSerializer.encodeInt(labelId)
    iter.seek(prefix)

    new Iterator[StoredNodeWithProperty]() {
      override def hasNext: Boolean =
        iter.isValid && iter.key().startsWith(prefix)
      override def next(): StoredNodeWithProperty = {
        val node = NodeSerializer.decodeNodeWithProperties(iter.value())
        iter.next()
        node
      }
    }
  }

  def getNodeIdsByLabel(labelId: LabelId): Iterator[NodeId] = {
    val iter = db.newIterator()
    val prefix = BaseSerializer.encodeInt(labelId)
    iter.seek(prefix)

    new Iterator[NodeId]() {
      override def hasNext: Boolean =
        iter.isValid && iter.key().startsWith(prefix)
      override def next(): NodeId = {
        val id = NodeSerializer.decodeNodeKey(iter.key())._1
        iter.next()
        id
      }
    }
  }

  def getNodesByLabelWithoutDeserialize(labelId: LabelId): Iterator[NodeId] = {
    val iter = db.newIterator()
    val prefix = BaseSerializer.encodeInt(labelId)
    iter.seek(prefix)

    new Iterator[NodeId]() {
      override def hasNext: Boolean =
        iter.isValid && iter.key().startsWith(prefix)
      override def next(): NodeId = {
        val id = NodeSerializer.decodeNodeKey(iter.key())._1
        iter.value().length
        iter.next()
        id
      }
    }
  }

  def deleteByLabel(labelId: LabelId): Unit =
    db.deleteRange(
      NodeSerializer.encodeNodeKey(0L, labelId),
      NodeSerializer.encodeNodeKey(-1L, labelId)
    )

  def delete(nodeId: NodeId, labelId: LabelId): Unit =
    db.delete(NodeSerializer.encodeNodeKey(nodeId, labelId))

  def delete(nodeId: Long, labelIds: Array[LabelId]): Unit =
    labelIds.foreach(delete(nodeId, _))

  def delete(node: StoredNodeWithProperty): Unit =
    delete(node.id, node.labelIds)
}
