package org.grapheco.lynx.logical

import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class LogicalLimit(expression: Expression)(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}
