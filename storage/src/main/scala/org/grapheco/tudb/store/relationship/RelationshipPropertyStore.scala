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

package org.grapheco.tudb.store.relationship

import org.grapheco.tudb.serializer.RelationshipSerializer
import org.grapheco.tudb.store.meta.TypeManager.RelationshipId
import org.grapheco.tudb.store.storage.KeyValueDB

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 7:44 下午 2022/2/2
  * @Modified By:
  */
class RelationshipPropertyStore(db: KeyValueDB) {

  def set(
      relationshipId: Long,
      fromId: Long,
      toId: Long,
      typeId: Int,
      props: Map[Int, Any]
    ): Unit = {
    val keyBytes: Array[Byte] =
      RelationshipSerializer.encodeRelationshipKey(relationshipId)
    val relationshipInBytes: Array[Byte] = RelationshipSerializer
      .encodeRelationship(relationshipId, fromId, toId, typeId, props)
    db.put(keyBytes, relationshipInBytes)
  }
  def set(relationship: StoredRelationshipWithProperty): Unit = {
    val keyBytes = RelationshipSerializer.encodeRelationshipKey(relationship.id)
    db.put(keyBytes, relationship.sourceBytes)
  }

  def set(relationship: StoredRelationship): Unit = {
    val keyBytes = RelationshipSerializer.encodeRelationshipKey(relationship.id)
    val relationshipInBytes: Array[Byte] =
      RelationshipSerializer.encodeRelationship(relationship)
    db.put(keyBytes, relationshipInBytes)
  }

  def delete(relationshipId: RelationshipId): Unit =
    db.delete(RelationshipSerializer.encodeRelationshipKey(relationshipId))

  def get(relationshipId: RelationshipId): Option[StoredRelationshipWithProperty] = {
    val keyBytes = RelationshipSerializer.encodeRelationshipKey(relationshipId)
    val res = db.get(keyBytes)
    if (res != null)
      Some(RelationshipSerializer.decodeRelationshipWithProperties(res))
    else
      None
  }

  def exist(relationshipId: RelationshipId): Boolean =
    db.get(RelationshipSerializer.encodeRelationshipKey(relationshipId)) != null

  def all(): Iterator[StoredRelationshipWithProperty] = {
    new Iterator[StoredRelationshipWithProperty] {
      val iter = db.newIterator()
      iter.seekToFirst()

      override def hasNext: Boolean = iter.isValid

      override def next(): StoredRelationshipWithProperty = {
        val relation =
          RelationshipSerializer.decodeRelationshipWithProperties(iter.value())
        iter.next()
        relation
      }
    }
  }

  def count: Long = {
    val iter = db.newIterator()
    iter.seekToFirst()
    var count: Long = 0
    while (iter.isValid) {
      count += 1
      iter.next()
    }
    count
  }

  def close(): Unit = {
    db.close()
  }
}
