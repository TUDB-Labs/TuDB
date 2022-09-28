package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.SortItem

/**
  *@description:
  */
case class LogicalOrderBy(sortItem: Seq[SortItem])(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}
