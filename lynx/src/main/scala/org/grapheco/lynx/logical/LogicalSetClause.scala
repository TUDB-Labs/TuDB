package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.SetClause

/**
  *@description:
  */
case class LogicalSetClause(d: SetClause)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
