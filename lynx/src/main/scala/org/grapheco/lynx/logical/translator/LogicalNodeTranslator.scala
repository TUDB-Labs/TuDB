package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.LogicalNode
import org.grapheco.lynx.planner.LogicalPlannerContext

/**
  *@description: translates an ASTNode into a LogicalNode, `in` as input operator
  */
trait LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode
}
