package org.grapheco.tudb.graph

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey}
import org.grapheco.tudb.store.node.NodeStoreSPI

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 17:27 2022/3/31
  * @Modified By:
  */
case class LynxNodeId(value: Long) extends LynxId {
  override def toLynxInteger: LynxInteger = LynxInteger(value)
}

case class TuNode(longId: Long, labels: Seq[LynxNodeLabel], props: Seq[(String, LynxValue)])
  extends LynxNode {
  lazy val properties: Map[String, LynxValue] = props.toMap
  override val id: LynxId = LynxNodeId(longId)
  def property(name: String): Option[LynxValue] = properties.get(name)

  override def toString: String =
    s"{<id>:${id.value}, labels:[${labels.mkString(",")}], properties:{${properties
      .map(kv => kv._1 + ": " + kv._2.value.toString)
      .mkString(",")}}"

  override def keys: Seq[LynxPropertyKey] =
    props.map(pair => LynxPropertyKey(pair._1))

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] =
    properties.get(propertyKey.value)
}

case class LazyTuNode(longId: Long, nodeStoreSPI: NodeStoreSPI) extends LynxNode {
  lazy val nodeValue: TuNode = transfer(nodeStoreSPI)
  override val id: LynxId = LynxNodeId(longId)

  override def labels: Seq[LynxNodeLabel] =
    nodeStoreSPI
      .getNodeLabelsById(longId)
      .map(f => LynxNodeLabel(nodeStoreSPI.getLabelName(f).get))
      .toSeq

  def property(name: String): Option[LynxValue] = nodeValue.properties.get(name)

  def transfer(nodeStore: NodeStoreSPI): TuNode = {
    val node = nodeStore.getNodeById(longId).get
    TuNode(
      node.id,
      node.labelIds
        .map((id: Int) => LynxNodeLabel(nodeStore.getLabelName(id).get))
        .toSeq,
      node.properties
        .map(kv =>
          (
            nodeStore.getPropertyKeyName(kv._1).getOrElse("unknown"),
            LynxValue(kv._2)
          )
        )
        .toSeq
    )
  }

  override def keys: Seq[LynxPropertyKey] =
    nodeValue.props.map(pair => LynxPropertyKey(pair._1))

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] =
    nodeValue.properties.get(propertyKey.value)
}
