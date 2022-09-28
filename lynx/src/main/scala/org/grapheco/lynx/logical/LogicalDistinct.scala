package org.grapheco.lynx.logical

/**
  *@description:
  */
case class LogicalDistinct()(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}
