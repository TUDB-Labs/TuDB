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

package org.grapheco.tudb.store.storage

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 5:44 下午 2022/1/28
  * @Modified By:
  */
trait KeyValueDB {
  def get(key: Array[Byte]): Array[Byte]
  def put(key: Array[Byte], value: Array[Byte]): Unit
  def write(option: Any, batch: Any): Unit // TODO option and batch trait
  def delete(key: Array[Byte]): Unit
  def deleteRange(key1: Array[Byte], key2: Array[Byte])
  def newIterator(): KeyValueIterator
  def flush(): Unit
  def close(): Unit
}

trait KeyValueIterator {
  def isValid: Boolean
  def seek(key: Array[Byte]): Unit
  def seekToFirst(): Unit
  def seekToLast(): Unit
  def seekForPrev(key: Array[Byte]): Unit
  def next(): Unit
  def prev(): Unit
  def key(): Array[Byte]
  def value(): Array[Byte]
}

trait KeyValueWriteBatch {
  def put(key: Array[Byte], value: Array[Byte]): Unit
}
