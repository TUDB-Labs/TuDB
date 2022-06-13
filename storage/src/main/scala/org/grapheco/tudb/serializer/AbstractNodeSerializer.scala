package org.grapheco.tudb.serializer

import org.grapheco.tudb.store.meta.TypeManager.{LabelId, NodeId}
import org.grapheco.tudb.store.node.StoredNodeWithProperty

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 4:33 下午 2022/1/23
  * @Modified By:
  */
trait AbstractNodeSerializer extends BaseSerializer {
  def encodeNodeKey(id: Long, labelIds: Array[Int]): Array[Array[Byte]]

  def encodeNodeKey(id: Long, labelId: Int): Array[Byte]

  def encodeNodeLabels(labelIds: Array[LabelId]): Array[Byte]

  def encodeNodeProperties(props: Map[Int, Any]): Array[Byte]

  def encodeNodeWithProperties(
      storedNodeWithProperty: StoredNodeWithProperty
  ): Array[Byte]

  def encodeNodeWithProperties(
      id: Long,
      labelIDs: Array[Int],
      properties: Map[Int, Any]
  ): Array[Byte]

  def decodeNodeWithProperties(bytes: Array[Byte]): StoredNodeWithProperty

  def decodePropertiesFromFullNode(bytes: Array[Byte]): Map[Int, Any]

  def decodeNodeKey(bytes: Array[Byte]): (NodeId, LabelId)
}
