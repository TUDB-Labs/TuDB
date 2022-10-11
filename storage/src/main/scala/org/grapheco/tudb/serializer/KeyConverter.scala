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

package org.grapheco.tudb.serializer

import io.netty.buffer.PooledByteBufAllocator

class IllegalKeyException(s: String) extends RuntimeException(s) {}

object KeyConverter extends BaseSerializer {
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
    val RelationType = Value(
      11
    ) // [KeyType(1Byte), LabelId(4Byte)] --> LabelName(String)
    val PropertyName = Value(
      12
    ) // [KeyType(1Byte),PropertyId(4Byte)] --> propertyName(String)

    val NodeIdGenerator = Value(13) // [keyType(1Byte)] -> MaxId(Long)
    val RelationIdGenerator = Value(14) // [keyType(1Byte)] -> MaxId(Long)
    val IndexIdGenerator = Value(15) // [keyType(1Byte)] -> MaxId(Long)
  }

  type NodeId = Long
  type RelationId = Long
  type LabelId = Int
  type TypeId = Int
  type PropertyId = Int
  type IndexId = Int
  type Value = Array[Byte]

  val NODE_ID_SIZE = 8
  val RELATION_ID_SIZE = 8
  val LABEL_ID_SIZE = 4
  val TYPE_ID_SIZE = 4
  val PROPERTY_ID_SIZE = 4
  val INDEX_ID_SIZE = 4
  val IS_FULLTEXT_SIZE = 1

  val allocator: PooledByteBufAllocator = PooledByteBufAllocator.DEFAULT

  // [keyType(1Bytes),id(4Bytes)]
  def nodeLabelKeyToBytes(labelId: Int): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeByte(KeyType.NodeLabel.id.toByte)
    _bytebuf.writeInt(labelId)
    releaseBuf(_bytebuf)
  }

  // [keyType(1Bytes),id(4Bytes)]
  def relationTypeKeyToBytes(typeId: Int): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeByte(KeyType.RelationType.id.toByte)
    _bytebuf.writeInt(typeId)
    releaseBuf(_bytebuf)
  }

  // [keyType(1Bytes),id(4Bytes)]
  def propertyNameKeyToBytes(propertyNameId: Int): Array[Byte] = {
    val _bytebuf = allocator.directBuffer()
    _bytebuf.writeByte(KeyType.PropertyName.id.toByte)
    _bytebuf.writeInt(propertyNameId)
    releaseBuf(_bytebuf)
  }

  // [keyType(1Bytes)]
  def nodeIdGeneratorKeyToBytes(): Array[Byte] = {
    Array(KeyType.NodeIdGenerator.id.toByte)
  }
  // [keyType(1Bytes)]
  def relationIdGeneratorKeyToBytes(): Array[Byte] = {
    Array(KeyType.RelationIdGenerator.id.toByte)
  }
  // [KeyType(1Bytes)]
  def indexIdGeneratorKeyToBytes(): Array[Byte] = {
    Array(KeyType.IndexIdGenerator.id.toByte)
  }

  /** ╔═════════════════════════════╗
    * ║              key            ║
    * ╠═══════╦═══════╦═════════════╣
    * ║ label ║ props ║   fullText  ║
    * ╚═══════╩═══════╩═════════════╝
    */
  def toIndexMetaKey(
      labelId: LabelId,
      props: Array[PropertyId],
      isFullText: Boolean
    ): Array[Byte] = {
    val bytes = new Array[Byte](
      LABEL_ID_SIZE + PROPERTY_ID_SIZE * props.length + IS_FULLTEXT_SIZE
    )
    ByteUtils.setInt(bytes, 0, labelId)
    var index = LABEL_ID_SIZE
    props.foreach { p =>
      ByteUtils.setInt(bytes, index, p)
      index += PROPERTY_ID_SIZE
    }
    ByteUtils.setBoolean(bytes, bytes.length - IS_FULLTEXT_SIZE, isFullText)
    bytes
  }

  def getIndexMetaFromKey(keyBytes: Array[Byte]): (LabelId, Array[PropertyId], Boolean) = {
    if (keyBytes.length < LABEL_ID_SIZE + PROPERTY_ID_SIZE + IS_FULLTEXT_SIZE)
      return null
    (
      ByteUtils.getInt(keyBytes, 0),
      (0 until (keyBytes.length - LABEL_ID_SIZE - IS_FULLTEXT_SIZE) / PROPERTY_ID_SIZE).toArray
        .map(i => ByteUtils.getInt(keyBytes, LABEL_ID_SIZE + PROPERTY_ID_SIZE * i)),
      ByteUtils.getBoolean(keyBytes, keyBytes.length - IS_FULLTEXT_SIZE)
    )
  }

  /** ╔══════════════════════════════════════════╗
    * ║                   key                    ║
    * ╠═════════╦══════════╦══════════╦══════════╣
    * ║ indexId ║ typeCode ║  value   ║  nodeId  ║
    * ╚═════════╩══════════╩══════════╩══════════╝
    */
  def toIndexKey(
      indexId: IndexId,
      typeCode: Byte,
      value: Array[Byte],
      nodeId: NodeId
    ): Array[Byte] = {
    val bytes = new Array[Byte](INDEX_ID_SIZE + NODE_ID_SIZE + 1 + value.length)
    ByteUtils.setInt(bytes, 0, indexId)
    ByteUtils.setByte(bytes, INDEX_ID_SIZE, typeCode)
    for (i <- value.indices)
      bytes(INDEX_ID_SIZE + 1 + i) = value(i)
    ByteUtils.setLong(bytes, bytes.length - NODE_ID_SIZE, nodeId)
    bytes
  }

  def toIndexKey(indexId: IndexId, typeCode: Byte, value: Array[Byte]): Array[Byte] = {
    val bytes = new Array[Byte](INDEX_ID_SIZE + 1 + value.length)
    ByteUtils.setInt(bytes, 0, indexId)
    ByteUtils.setByte(bytes, INDEX_ID_SIZE, typeCode)
    for (i <- value.indices)
      bytes(INDEX_ID_SIZE + 1 + i) = value(i)
    bytes
  }

  def toIndexKey(indexId: IndexId, typeCode: Byte): Array[Byte] = {
    val bytes = new Array[Byte](INDEX_ID_SIZE + 1)
    ByteUtils.setInt(bytes, 0, indexId)
    ByteUtils.setByte(bytes, INDEX_ID_SIZE, typeCode)
    bytes
  }

}
