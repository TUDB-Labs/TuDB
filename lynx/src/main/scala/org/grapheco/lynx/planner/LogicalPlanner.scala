package org.grapheco.lynx.planner

import org.grapheco.lynx.logical.LogicalNode
import org.opencypher.v9_0.ast.Statement

/**
  *@description:
  */
trait LogicalPlanner {
  def plan(statement: Statement, plannerContext: LogicalPlannerContext): LogicalNode
}
