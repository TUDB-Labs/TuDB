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

import org.grapheco.tudb.serializer.{BaseSerializer, RelationshipSerializer}
import org.grapheco.tudb.store.storage.KeyValueDB

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 12:42 下午 2022/2/4
  * @Modified By:
  */
class RelationshipLabelIndex(db: KeyValueDB) {

  def set(labelId: Int, relId: Long): Unit = {
    val keyBytes =
      RelationshipSerializer.encodeRelationshipTypeKey(labelId, relId)
    db.put(keyBytes, Array.emptyByteArray)
  }

  def delete(labelId: Int, relId: Long): Unit = {
    val keyBytes =
      RelationshipSerializer.encodeRelationshipTypeKey(labelId, relId)
    db.delete(keyBytes)
  }

  def getRelations(labelId: Int): Iterator[Long] = {
    val keyPrefix = RelationshipSerializer.encodeRelationshipTypeKey(labelId)
    val iter = db.newIterator()
    iter.seek(keyPrefix)

    new Iterator[Long]() {
      override def hasNext: Boolean =
        iter.isValid && iter.key().startsWith(keyPrefix)

      override def next(): Long = {
        val relId: Long =
          BaseSerializer.decodeLong(iter.key(), keyPrefix.length)
        iter.next()
        relId
      }
    }
  }

  def close(): Unit = db.close()

}
