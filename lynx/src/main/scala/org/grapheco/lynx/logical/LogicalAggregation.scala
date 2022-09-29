package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.ReturnItem

/**
  *@description:
  */
case class LogicalAggregation(
    aggregations: Seq[ReturnItem],
    groupings: Seq[ReturnItem]
  )(val in: LogicalNode)
  extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}
