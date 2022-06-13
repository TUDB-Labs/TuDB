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
    props: Seq[(String, LynxValue)]
) extends LynxRelationship {
  lazy val properties = props.toMap
  override val id: LynxId = LynxRelationshipId(_id)
  override val startNodeId: LynxId = LynxNodeId(startId)
  override val endNodeId: LynxId = LynxNodeId(endId)

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] =
    properties.get(propertyKey.value)

  override def keys: Seq[LynxPropertyKey] =
    props.map(pair => LynxPropertyKey(pair._1))
}
