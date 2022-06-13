package org.grapheco.tudb.store.meta

import org.grapheco.tudb.serializer.MetaDataSerializer
import org.grapheco.tudb.store.meta.TypeManager.KeyType
import org.grapheco.tudb.store.storage.KeyValueDB

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 12:21 下午 2022/2/1
  * @Modified By:
  */

class PropertyNameStore(kvDB: KeyValueDB) extends NameStore {
  override val db: KeyValueDB = kvDB
  override val key2ByteArrayFunc: Int => Array[Byte] =
    MetaDataSerializer.encodePropertyIdKey
  override val keyPrefixFunc: () => Array[Byte] = () =>
    Array(KeyType.PropertyName.id.toByte)
  override val initInt: Int = 200000
  loadAll()

}
