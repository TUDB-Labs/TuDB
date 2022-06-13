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
