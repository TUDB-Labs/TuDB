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

package org.grapheco.tudb.graph

import org.grapheco.tudb.serializer.{BaseSerializer, RelationshipSerializer}

import collection.mutable.Map
import scala.collection.immutable.{Map => IMutableMap}

object Relationship {
  def loads(bytes: Array[Byte]): Relationship = {
    val allocator = RelationshipSerializer.allocator
    val byteBuffer = allocator.directBuffer()
    byteBuffer.writeBytes(bytes)
    val relationshipId: Long = byteBuffer.readLong()
    val fromID: Long = byteBuffer.readLong()
    val toID: Long = byteBuffer.readLong()
    val typeID: Int = byteBuffer.readInt()
    val properties: IMutableMap[Int, Any] = BaseSerializer.decodePropMap(byteBuffer)
    byteBuffer.release()
    new Relationship(relationshipId, fromID, toID, typeID, Map(properties.toSeq: _*))
  }
}

class Relationship(id: Long, from: Long, to: Long, typeId: Int, properties: Map[Int, Any]) {

  def addProperty(key: Int, value: Any) = {
    properties(key) = value
  }

  def property(key: Int): Option[Any] = {
    properties.get(key)
  }

  // FIXME: refactor to a general interface such as serializable
  def dumps(): Array[Byte] = {
    RelationshipSerializer.encodeRelationship(id, from, to, typeId, properties.toMap)
  }

  override def toString: String =
    s"<relationId: $id, relationType:$typeId, properties:{${properties.toList.mkString(",")}}>"
}
