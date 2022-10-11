// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.lynx

import org.grapheco.lynx.physical.PhysicalNode
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical.rules.{ExtractJoinReferenceRule, JoinTableSizeEstimateRule, JoinToExpandRule, PhysicalFilterPushDownRule, RemoveNullProject}

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
