package org.grapheco.lynx.logical

import org.grapheco.lynx.TreeNode

/**
  *@description: logical plan tree node (operator)
  */
trait LogicalNode extends TreeNode {
  override type SerialType = LogicalNode
  override val children: Seq[LogicalNode] = Seq.empty
}

trait LogicalQueryPart extends LogicalNode
