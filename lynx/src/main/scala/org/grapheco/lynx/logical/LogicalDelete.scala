package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.Delete

/**
  *@description:
  */
case class LogicalDelete(delete: Delete)(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}
