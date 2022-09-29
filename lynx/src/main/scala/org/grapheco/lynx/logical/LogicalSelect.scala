package org.grapheco.lynx.logical

/**
  *@description:
  */
case class LogicalSelect(columns: Seq[(String, Option[String])])(val in: LogicalNode)
  extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}
