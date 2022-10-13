// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.tudb.store.node

import com.typesafe.scalalogging.LazyLogging

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 9:57 下午 2022/1/25
  * @Modified By:
  */
trait NodeStoreSPI extends LazyLogging {

  /**
    * filter property query node id by index engine
    * @param propertyKey
    * @param propertyValue
    * @return
    */
  def getNodeIdByProperty(propertyKey: Int, propertyValue: Any): Set[Long]

  /**
    * report whether the index engine is enable for the storage
    * @return bool
    */
  def hasIndex(): Boolean

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

  def getNodeById(nodeId: Long): Option[StoredNodeWithProperty]

  def getNodeById(nodeId: Long, label: Int): Option[StoredNodeWithProperty]

  def getNodeById(nodeId: Long, label: Option[Int]): Option[StoredNodeWithProperty]

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
