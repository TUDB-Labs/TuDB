package org.grapheco.tudb.engine
import org.grapheco.tudb.graph.Node

import com.typesafe.scalalogging.LazyLogging

trait NodeOperations extends LazyLogging {

  def getNodeIdByProperty(propertyKey: Int, propertyValue: Any): Set[Long]

  def hasIndex(): Boolean

  def refreshMeta(): Unit

  def newNodeId(): Long;

  def cleanData(): Unit

  def hasLabel(nodeId: Long, label: Int): Boolean;

  def addNode(node: Node): Unit

  def addNode(nodeId: Long, labelIds: Array[Int], props: Map[Int, Any]): Unit

  def addLabel(labelName: String): Int;

  def addPropertyKey(keyName: String): Int;

  def nodeAddLabel(nodeId: Long, labelId: Int): Unit;

  def nodeRemoveLabel(nodeId: Long, labelId: Int): Unit;

  def nodeSetProperty(nodeId: Long, propertyKeyId: Int, propertyValue: Any): Unit;

  def nodeRemoveProperty(nodeId: Long, propertyKeyId: Int): Any;

  def deleteNode(nodeId: Long): Unit;

  def deleteNodes(nodeIDs: Iterator[Long]): Unit;

  def deleteNodesByLabel(labelId: Int): Unit

  def getLabelName(labelId: Int): Option[String];

  def getLabelId(labelName: String): Option[Int];

  def getLabelIds(labelNames: Set[String]): Set[Int]

  def getPropertyKeyName(keyId: Int): Option[String];

  def getPropertyKeyId(keyName: String): Option[Int];

  def getNodeById(nodeId: Long): Option[Node]

  def getNodeById(nodeId: Long, label: Int): Option[Node]

  def getNodeById(nodeId: Long, label: Option[Int]): Option[Node]

  def getNodesByLabel(labelId: Int): Iterator[Node];
  def getNodesByLabels(labelIds: Seq[Int]): Iterator[Node]

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

  def allNodes(): Iterator[Node]

  def nodesCount: Long

  def close(): Unit
}
