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

package org.grapheco.tudb.graph

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxId, LynxPropertyKey, LynxRelationship, LynxRelationshipType}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 17:28 2022/3/31
  * @Modified By:
  */
case class LynxRelationshipId(value: Long) extends LynxId {
  override def toLynxInteger: LynxInteger = LynxInteger(value)
}

case class TuRelationship(
    _id: Long,
    startId: Long,
    endId: Long,
    relationType: Option[LynxRelationshipType],
    props: Seq[(String, LynxValue)])
  extends LynxRelationship {
  lazy val properties = props.toMap
  override val id: LynxId = LynxRelationshipId(_id)
  override val startNodeId: LynxId = LynxNodeId(startId)
  override val endNodeId: LynxId = LynxNodeId(endId)

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] =
    properties.get(propertyKey.value)

  override def keys: Seq[LynxPropertyKey] =
    props.map(pair => LynxPropertyKey(pair._1))
}
