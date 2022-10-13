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
  override val keyPrefixFunc: () => Array[Byte] = () => Array(KeyType.NodeLabel.id.toByte)
  override val initInt: Int = 100000
  loadAll()
}
