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
import org.grapheco.tudb.store.meta.TypeManager.{KeyType, LabelId, PropertyId}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 6:46 下午 2022/2/1
  * @Modified By:
  */
object MetaDataSerializer extends BaseSerializer {
  private val _allocator: PooledByteBufAllocator =
    PooledByteBufAllocator.DEFAULT

  def decodeNameIdFromMetaKey(bytes: Array[Byte]): Int = {
    val _bytebuf = _allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    _bytebuf.readByte()
    val nameId: Int = _bytebuf.readInt()
    _bytebuf.clear()
    nameId
  }

  def encodePropertyIdKey(propertyId: PropertyId): Array[Byte] = {
    val _bytebuf = _allocator.directBuffer()
    _bytebuf.writeByte(KeyType.PropertyName.id.toByte)
    _bytebuf.writeInt(propertyId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def encodeNodeLabelKey(labelId: LabelId): Array[Byte] = {
    val _bytebuf = _allocator.directBuffer()
    _bytebuf.writeByte(KeyType.NodeLabel.id.toByte)
    _bytebuf.writeInt(labelId)
    BaseSerializer.releaseBuf(_bytebuf)
  }

  def decodeCurrentId(bytes: Array[Byte]): Long = {
    val _bytebuf = _allocator.directBuffer()
    _bytebuf.writeBytes(bytes)
    val currentId: Long = _bytebuf.readLong()
    _bytebuf.clear()
    currentId
  }

}
