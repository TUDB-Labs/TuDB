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

package org.grapheco.tudb.store.meta

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 12:21 下午 2022/1/29
  * @Modified By:
  */
object TypeManager {
  type NodeId = Long
  type RelationshipId = Long
  type LabelId = Int
  type TypeId = Int
  type PropertyId = Int
  type IndexId = Int
  type Value = Array[Byte]

  object KeyType extends Enumeration {
    type KeyType = Value

    val Node = Value(
      1
    ) // [keyType(1Byte),nodeId(8Bytes)] -> nodeValue(id, labels, properties)
    val NodeLabelIndex = Value(
      2
    ) // [keyType(1Byte),labelId(4Bytes),nodeId(8Bytes)] -> null
    val NodePropertyIndexMeta = Value(
      3
    ) // [keyType(1Byte),labelId(4Bytes),properties(x*4Bytes)] -> null
    val NodePropertyIndex = Value(
      4
    ) // // [keyType(1Bytes),indexId(4),propValue(xBytes),valueLength(xBytes),nodeId(8Bytes)] -> null

    val Relation = Value(
      5
    ) // [keyType(1Byte),relationId(8Bytes)] -> relationValue(id, fromNode, toNode, labelId, category)
    val RelationLabelIndex = Value(
      6
    ) // [keyType(1Byte),labelId(4Bytes),relationId(8Bytes)] -> null
    val OutEdge = Value(
      7
    ) // [keyType(1Byte),fromNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),toNodeId(8Bytes)] -> relationValue(id,properties)
    val InEdge = Value(
      8
    ) // [keyType(1Byte),toNodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),fromNodeId(8Bytes)] -> relationValue(id,properties)
    val NodePropertyFulltextIndexMeta = Value(
      9
    ) // [keyType(1Byte),labelId(4Bytes),properties(x*4Bytes)] -> null

    val NodeLabel = Value(
      10
    ) // [KeyType(1Byte), LabelId(4Byte)] --> LabelName(String)
    val RelationshipType = Value(
      11
    ) // [KeyType(1Byte), LabelId(4Byte)] --> LabelName(String)
    val PropertyName = Value(
      12
    ) // [KeyType(1Byte),PropertyId(4Byte)] --> propertyName(String)

    val NodeIdGenerator = Value(13) // [keyType(1Byte)] -> MaxId(Long)
    val RelationIdGenerator = Value(14) // [keyType(1Byte)] -> MaxId(Long)
    val IndexIdGenerator = Value(15) // [keyType(1Byte)] -> MaxId(Long)

  }

}
