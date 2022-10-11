package org.grapheco.lynx.physical.rules

import org.grapheco.lynx.PhysicalPlanOptimizerRule
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical.{PhysicalNode, PhysicalProject}
import org.opencypher.v9_0.ast.AliasedReturnItem

/**
  *@description:
  */
object RemoveNullProject extends PhysicalPlanOptimizerRule {
  override def apply(plan: PhysicalNode, ppc: PhysicalPlannerContext): PhysicalNode =
    optimizeBottomUp(
      plan, {
        case pnode: PhysicalNode =>
          pnode.children match {
            case Seq(p @ PhysicalProject(ri)) if ri.items.forall {
                  case AliasedReturnItem(expression, variable) => expression == variable
                } =>
              pnode.withChildren(pnode.children.filterNot(_ eq p) ++ p.children)

            case _ => pnode
          }
      }
    )
}