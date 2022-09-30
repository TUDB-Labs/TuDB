package org.grapheco.lynx.physical.plan

import org.grapheco.lynx.logical.LogicalNode
import org.grapheco.lynx.physical.PhysicalNode

/**
  *@description:
  */
trait PhysicalPlanner {
  def plan(logicalPlan: LogicalNode)(implicit plannerContext: PhysicalPlannerContext): PhysicalNode
}