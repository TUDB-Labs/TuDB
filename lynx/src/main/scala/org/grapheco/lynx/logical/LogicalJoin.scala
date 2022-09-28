package org.grapheco.lynx.logical

/**
  *@description:
  */
case class LogicalJoin(val isSingleMatch: Boolean)(val a: LogicalNode, val b: LogicalNode)
  extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(a, b)
}
