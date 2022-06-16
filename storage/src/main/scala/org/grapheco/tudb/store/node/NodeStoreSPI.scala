package org.grapheco.tudb.store.node

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 9:57 下午 2022/1/25
  * @Modified By:
  */
trait NodeStoreSPI {
  def getNodeIdByProperty(value:Any): Set[Long]
  def hasIndex():Boolean

  def refreshMeta(): Unit

  def newNodeId(): Long;

  def cleanData(): Unit

  def hasLabel(nodeId: Long, label: Int): Boolean;

  def addNode(node: StoredNodeWithProperty): Unit

  def addNode(nodeId: Long, labelIds: Array[Int], props: Map[Int, Any]): Unit

  def addLabel(labelName: String): Int;

  def addPropertyKey(keyName: String): Int;

  def nodeAddLabel(nodeId: Long, labelId: Int): Unit;

  def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit;

  def nodeSetProperty(
      nodeId: Long,
      propertyKeyId: Int,
      propertyValue: Any
  ): Unit;

  def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any;

  def deleteNode(nodeId: Long): Unit;

  def deleteNodes(nodeIDs: Iterator[Long]): Unit;

  def deleteNodesByLabel(labelId: Int): Unit

  def getLabelName(labelId: Int): Option[String];

  def getLabelId(labelName: String): Option[Int];

  def getLabelIds(labelNames: Set[String]): Set[Int]

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Option[Int];

  def getNodeById(nodeId: Long): Option[StoredNodeWithProperty]

  def getNodeById(nodeId: Long, label: Int): Option[StoredNodeWithProperty]

  def getNodeById(
      nodeId: Long,
      label: Option[Int]
  ): Option[StoredNodeWithProperty]

  def getNodesByLabel(labelId: Int): Iterator[StoredNodeWithProperty];
  def getNodesByLabels(labelIds: Seq[Int]): Iterator[StoredNodeWithProperty]

//  def getNodesByIds(labelId: Int, ids: Seq[Long]): Iterator[StoredNodeWithProperty];

  def getNodeIdsByLabel(labelId: Int): Iterator[Long];

  def getNodeLabelsById(nodeId: Long): Array[Int];

  def serializeLabelIdsToBytes(labelIds: Array[Int]): Array[Byte];

  def deserializeBytesToLabelIds(bytes: Array[Byte]): Array[Int];

  def serializePropertiesToBytes(properties: Map[Int, Any]): Array[Byte];

  def deserializeBytesToProperties(bytes: Array[Byte]): Map[Int, Any];

  def allLabels(): Array[String];

  def allLabelIds(): Array[Int];

  def allPropertyKeys(): Array[String];

  def allPropertyKeyIds(): Array[Int];

  def allNodes(): Iterator[StoredNodeWithProperty]

  def nodesCount: Long

  def close(): Unit
}
