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
import org.grapheco.tudb.store.storage.KeyValueDB

import java.util.concurrent.atomic.AtomicLong

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 8:57 下午 2022/2/1
  * @Modified By:
  */
class IdGenerator(val db: KeyValueDB, val sequenceSize: Int) {

  //  val keyBytes: Array[Byte]
  private val id: AtomicLong = {
    val iter = db.newIterator()
    iter.seekToLast()
    val value: Array[Byte] = iter.key()
    if (value == null) {
      new AtomicLong(0)
    } else {
      val current: Long =
        if (value.length < 8) 0L else MetaDataSerializer.decodeCurrentId(value)
      new AtomicLong(current)
    }
  }

  private val bound = new AtomicLong(0)

  private def slideDown(current: Long): Unit = {
    val end = current + sequenceSize - 1
    //    writeId(end)
    bound.set(end)
  }

  private def slideDownIfNeeded(nid: Long): Unit = {
    if (nid > bound.get()) {
      slideDown(nid)
    }
  }

  def currentId() = id.get()

  def update(newId: Long): Unit = {
    if (newId < currentId()) {
      throw new LowerIdSetException(newId)
    }

    id.set(newId)
    slideDownIfNeeded(newId)
  }

  def nextId(): Long = {
    val nid = id.incrementAndGet()
    //all ids consumed
    slideDownIfNeeded(nid)
    nid
  }
}

class LowerIdSetException(id: Long) extends RuntimeException(s"lower id set: $id") {}
