package org.grapheco.tudb.store.meta

import org.grapheco.tudb.serializer.RelationshipSerializer
import org.grapheco.tudb.store.storage.KeyValueDB

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 3:49 下午 2022/2/4
  * @Modified By:
  */
class RelationshipTypeNameStore(kvDB: KeyValueDB) extends NameStore {
  override val db: KeyValueDB = kvDB
  override val key2ByteArrayFunc: Int => Array[Byte] =
    RelationshipSerializer.encodeRelationshipTypeName
  override val keyPrefixFunc: () => Array[Byte] =
    RelationshipSerializer.relationshipTypeNameKeyPrefix
  override val initInt: Int = 300000
  loadAll()
}
