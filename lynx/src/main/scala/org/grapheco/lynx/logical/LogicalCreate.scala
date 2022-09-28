package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.Create

/**
  *@description:
  */
case class LogicalCreate(c: Create)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
