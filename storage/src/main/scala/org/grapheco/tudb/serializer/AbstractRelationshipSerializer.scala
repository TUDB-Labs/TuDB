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

import org.grapheco.tudb.store.relationship.{StoredRelationship, StoredRelationshipWithProperty}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 4:33 下午 2022/1/23
  * @Modified By:
  */
trait AbstractRelationshipSerializer {
  def encodeRelationship(
      relationId: Long,
      fromId: Long,
      toId: Long,
      typeId: Int,
      props: Map[Int, Any]
    ): Array[Byte]
  def encodeRelationship(storedRelationship: StoredRelationship): Array[Byte]

  def decodeRelationshipWithProperties(bytes: Array[Byte]): StoredRelationshipWithProperty
  def decodePropertiesFromFullRelationship(bytes: Array[Byte]): Map[Int, Any]

}
