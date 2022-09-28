package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.Remove

/**
  *@description:
  */
case class LogicalRemove(r: Remove)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
