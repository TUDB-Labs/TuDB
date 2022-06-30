package org.grapheco.tudb.store.relationship

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxId, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.tudb.serializer.RelationshipSerializer
import org.grapheco.tudb.store.node.LynxNodeId

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 7:24 上午 2022/1/26
  * @Modified By:
  */
case class StoredRelationship(
    id: Long,
    from: Long,
    to: Long,
    typeId: Int,
    sourceBytes: Array[Byte] = Array.emptyByteArray) {
  lazy val properties: Map[Int, Any] = Map.empty
}

class StoredRelationshipWithProperty(
    override val id: Long,
    override val from: Long,
    override val to: Long,
    override val typeId: Int,
    override val sourceBytes: Array[Byte])
  extends StoredRelationship(id, from, to, typeId, sourceBytes) {

  def this(id: Long, from: Long, to: Long, typeId: Int, props: Map[Int, Any]) = {
    this(
      id,
      from,
      to,
      typeId,
      RelationshipSerializer.encodeRelationship(id, from, to, typeId, props)
    )
  }

  override lazy val properties: Map[Int, Any] =
    RelationshipSerializer.decodePropertiesFromFullRelationship(sourceBytes)
  def invert() =
    new StoredRelationshipWithProperty(id, to, from, typeId, sourceBytes)

  override def toString: String =
    s"<relationId: $id, relationType:$typeId, properties:{${properties.toList.mkString(",")}}>"
}

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
