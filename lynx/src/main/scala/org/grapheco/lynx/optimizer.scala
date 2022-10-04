package org.grapheco.lynx

import org.grapheco.lynx.physical.PhysicalNode
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.rules.{ExtractJoinReferenceRule, JoinTableSizeEstimateRule, JoinToExpandRule, PhysicalFilterPushDownRule, RemoveNullProject}

trait PhysicalPlanOptimizer {
  def optimize(plan: PhysicalNode, ppc: PhysicalPlannerContext): PhysicalNode
}

trait PhysicalPlanOptimizerRule {
  def apply(plan: PhysicalNode, ppc: PhysicalPlannerContext): PhysicalNode

  def optimizeBottomUp(
      node: PhysicalNode,
      ops: PartialFunction[PhysicalNode, PhysicalNode]*
    ): PhysicalNode = {
    val childrenOptimized =
      node.withChildren(node.children.map(child => optimizeBottomUp(child, ops: _*)))
    ops.foldLeft(childrenOptimized) { (optimized, op) =>
      op.lift(optimized).getOrElse(optimized)
    }
  }
}

class DefaultPhysicalPlanOptimizer(runnerContext: CypherRunnerContext)
  extends PhysicalPlanOptimizer {
  val rules = Seq[PhysicalPlanOptimizerRule](
    RemoveNullProject,
    PhysicalFilterPushDownRule,
//    JoinToExpandRule,
    ExtractJoinReferenceRule,
    JoinTableSizeEstimateRule
  )

  def optimize(plan: PhysicalNode, ppc: PhysicalPlannerContext): PhysicalNode = {
    rules.foldLeft(plan)((optimized, rule) => rule.apply(optimized, ppc))
  }
}
