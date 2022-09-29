package org.grapheco.lynx

import org.grapheco.lynx.logical.LogicalNode
import org.grapheco.lynx.physical.PhysicalNode
import org.opencypher.v9_0.ast.Statement

/**
  *@description:
  */
trait PlanAware {
  def getASTStatement(): (Statement, Map[String, Any])

  def getLogicalPlan(): LogicalNode

  def getPhysicalPlan(): PhysicalNode

  def getOptimizerPlan(): PhysicalNode
}
