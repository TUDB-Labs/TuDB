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

import org.grapheco.tudb.serializer.NodeSerializer
import org.grapheco.tudb.store.meta.TypeManager.{LabelId, NodeId}
import org.grapheco.tudb.store.storage.KeyValueDB

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 10:58 下午 2022/1/31
  * @Modified By:
  */
class NodeLabelStore(db: KeyValueDB) {
  // [nodeId,labelId]->[null]

  def set(nodeId: NodeId, labelId: LabelId): Unit =
    db.put(
      NodeSerializer.encodeNodeLabelKey(nodeId, labelId),
      Array.emptyByteArray
    )

  def set(nodeId: NodeId, labels: Array[LabelId]): Unit =
    labels.foreach(set(nodeId, _))

  def delete(nodeId: NodeId, labelId: LabelId): Unit =
    db.delete(NodeSerializer.encodeNodeLabelKey(nodeId, labelId))

  def delete(nodeId: NodeId): Unit =
    db.deleteRange(
      NodeSerializer.encodeNodeLabelKey(nodeId, 0),
      NodeSerializer.encodeNodeLabelKey(nodeId, -1)
    )

  def get(nodeId: NodeId): Option[LabelId] = {
    val keyPrefix = NodeSerializer.encodeNodeLabelKey(nodeId)
    val iter = db.newIterator()
    iter.seek(keyPrefix)
    if (iter.isValid && iter.key().startsWith(keyPrefix))
      Some(NodeSerializer.decodeLabelIdInNodeLabelKey(iter.key()))
    else None
  }

  def exist(nodeId: NodeId, label: LabelId): Boolean = {
    val key = NodeSerializer.encodeNodeLabelKey(nodeId, label)
    db.get(key) != null
  }

  def getAll(nodeId: NodeId): Array[LabelId] = {
    val keyPrefix = NodeSerializer.encodeNodeLabelKey(nodeId)
    val iter = db.newIterator()
    iter.seek(keyPrefix)
    new Iterator[LabelId]() {
      override def hasNext: Boolean =
        iter.isValid && iter.key().startsWith(keyPrefix)

      override def next(): LabelId = {
        val label: LabelId =
          NodeSerializer.decodeLabelIdInNodeLabelKey(iter.key())
        iter.next()
        label
      }
    }.toArray
  }

  def getNodesCount: Long = {
    val iter = db.newIterator()
    iter.seekToFirst()
    var count: Long = 0
    var currentNode: Long = 0
    while (iter.isValid) {
      val id = NodeSerializer.decodeNodeIdInNodeLabelKey(iter.key())
      if (currentNode != id) {
        currentNode = id
        count += 1
      }
      iter.next()
    }
    count
  }
}
