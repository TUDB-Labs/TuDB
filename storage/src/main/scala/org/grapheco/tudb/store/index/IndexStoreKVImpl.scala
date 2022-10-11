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

package org.grapheco.tudb.store.index

import org.grapheco.tudb.serializer.KeyConverter
import org.grapheco.tudb.store.storage.KeyValueDB
import org.rocksdb.{WriteBatch, WriteOptions}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 17:07 2022/4/24
  * @Modified By:
  */
// This Class is used to natively build property index.
// It's heavy and expensive.
// We had better discard the native index function.
class IndexStoreKVImpl(db: KeyValueDB) {

  type IndexId = Int
  type NodeId = Long

  /**
    * Single Column Index:
    * ╔══════════════════════════════════════════╗
    * ║                   key                    ║
    * ╠═════════╦══════════╦══════════╦══════════╣
    * ║ indexId ║ typeCode ║  value   ║  nodeId  ║
    * ╚═════════╩══════════╩══════════╩══════════╝
    */
  def set(indexId: IndexId, typeCode: Byte, value: Array[Byte], nodeId: NodeId): Unit = {
    db.put(KeyConverter.toIndexKey(indexId, typeCode, value, nodeId), Array.emptyByteArray)
  }

  def set(indexId: IndexId, data: Iterator[(Any, Long)]): Unit = {
    val writeOpt = new WriteOptions()
    val batch = new WriteBatch()
    var i = 0
    while (data.hasNext) {
      val d = data.next()
      batch.put(
        KeyConverter.toIndexKey(
          indexId,
          IndexEncoder.typeCode(d._1),
          IndexEncoder.encode(d._1),
          d._2
        ),
        Array.emptyByteArray
      )
      if (i % 100000 == 0) {
        db.write(writeOpt, batch)
        batch.clear()
      }
      i += 1
    }
    db.write(writeOpt, batch)
  }

  def delete(indexId: IndexId, typeCode: Byte, value: Array[Byte], nodeId: NodeId): Unit = {
    db.delete(KeyConverter.toIndexKey(indexId, typeCode, value, nodeId))
  }

  def deleteRange(indexId: IndexId): Unit = {
    db.deleteRange(
      KeyConverter.toIndexKey(indexId, 0, Array.emptyByteArray, 0.toLong),
      KeyConverter.toIndexKey(
        indexId,
        Byte.MaxValue,
        Array.emptyByteArray,
        -1.toLong
      )
    )
  }

  def update(
      indexId: IndexId,
      typeCode: Byte,
      value: Array[Byte],
      nodeId: NodeId,
      newTypeCode: Byte,
      newValue: Array[Byte]
    ): Unit = {
    delete(indexId, typeCode, value, nodeId)
    set(indexId, newTypeCode, newValue, nodeId)
  }

}
