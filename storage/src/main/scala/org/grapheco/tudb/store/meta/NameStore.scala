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

import org.grapheco.tudb.serializer.{BaseSerializer, MetaDataSerializer}
import org.grapheco.tudb.store.storage.KeyValueDB

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 12:21 下午 2022/2/1
  * @Modified By:
  */
trait NameStore {
  val db: KeyValueDB
  val initInt: Int
  val key2ByteArrayFunc: (Int) => Array[Byte]
  val keyPrefixFunc: () => Array[Byte]

  var idGenerator: AtomicInteger = new AtomicInteger(initInt)
  var mapString2Int: mutable.Map[String, Int] = mutable.Map[String, Int]()
  var mapInt2String: mutable.Map[Int, String] = mutable.Map[Int, String]()

  private def addToDB(labelName: String): Int = {
    val id = idGenerator.incrementAndGet()
    mapString2Int += labelName -> id
    mapInt2String += id -> labelName
    val key = key2ByteArrayFunc(id)
    db.put(key, BaseSerializer.encodeString(labelName))
    id
  }

  def key(id: Int): Option[String] = mapInt2String.get(id)

  def id(labelName: String): Option[Int] = mapString2Int.get(labelName)

  def getOrAddId(labelName: String): Int =
    id(labelName).getOrElse(addToDB(labelName))

  def ids(keys: Set[String]): Set[Int] = {
    val newIds = keys.map { key =>
      val opt = mapString2Int.get(key)
      if (opt.isDefined) {
        opt.get
      } else {
        addToDB(key)
      }
    }
    newIds
  }

  def delete(labelName: String): Unit = {
    val id = mapString2Int(labelName)
    mapString2Int -= labelName
    mapInt2String -= id
    val key = key2ByteArrayFunc(id)
    db.delete(key)
  }

  // todo: This func should be confirmed.
  def loadAll(): Unit = {
    idGenerator = new AtomicInteger(initInt)
    mapString2Int = mutable.Map[String, Int]()
    mapInt2String = mutable.Map[Int, String]()
    var maxId: Int = initInt
    val prefix = keyPrefixFunc()
    val iter = db.newIterator()
    iter.seek(prefix)

    while (iter.isValid && iter.key().startsWith(prefix)) {
      val key = iter.key()
      val id = MetaDataSerializer.decodeNameIdFromMetaKey(key)
      if (maxId < id) maxId = id
      val name = BaseSerializer.decodeStringWithFlag(db.get(key))
      mapString2Int += name -> id
      mapInt2String += id -> name
      iter.next()
    }
    idGenerator.set(maxId)
  }

  def close(): Unit = {
    db.close()
  }
}
