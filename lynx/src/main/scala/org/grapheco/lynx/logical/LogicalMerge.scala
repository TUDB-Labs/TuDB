package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.{Merge, MergeAction}

/**
  *@description:
  */
case class LogicalMerge(m: Merge)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
case class LogicalMergeAction(m: Seq[MergeAction])(val in: Option[LogicalNode])
  extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
