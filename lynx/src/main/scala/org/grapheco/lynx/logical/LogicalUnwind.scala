package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.Unwind

/**
  *@description:
  */
case class LogicalUnwind(u: Unwind)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
