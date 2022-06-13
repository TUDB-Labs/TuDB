package org.grapheco.tudb.store.meta

import org.grapheco.tudb.serializer.MetaDataSerializer
import org.grapheco.tudb.store.meta.TypeManager.KeyType
import org.grapheco.tudb.store.storage.KeyValueDB

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 12:19 下午 2022/2/1
  * @Modified By:
  */
class NodeLabelNameStore(kvDB: KeyValueDB) extends NameStore {
  override val db: KeyValueDB = kvDB
  override val key2ByteArrayFunc: Int => Array[Byte] =
    MetaDataSerializer.encodeNodeLabelKey
  override val keyPrefixFunc: () => Array[Byte] = () =>
    Array(KeyType.NodeLabel.id.toByte)
  override val initInt: Int = 100000
  loadAll()
}
