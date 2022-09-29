package org.grapheco.lynx.logical

import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

/**
  *@description:
  */
case class LogicalPatternMatch(
    headNode: NodePattern,
    chain: Seq[(RelationshipPattern, NodePattern)])
  extends LogicalNode {}
