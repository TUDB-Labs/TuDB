package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.ReturnItemsDef

/**
  *@description:
  */
case class LogicalProject(ri: ReturnItemsDef)(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}
