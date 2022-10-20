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

package org.grapheco.lynx.physical.rules

import org.grapheco.lynx.PhysicalPlanOptimizerRule
import org.grapheco.lynx.expression.LynxVariable
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical.{PhysicalNode, PhysicalProject}

/**
  *@description:
  */
object RemoveNullProject extends PhysicalPlanOptimizerRule {
  override def apply(plan: PhysicalNode, ppc: PhysicalPlannerContext): PhysicalNode =
    optimizeBottomUp(
      plan, {
        case pnode: PhysicalNode =>
          pnode.children match {
            case Seq(p @ PhysicalProject(projectItems)) if projectItems.forall {
                  case (name, expression) => {
                    expression match {
                      case variable: LynxVariable => name == variable.name

                      case _ => false
                    }
                  }
                } =>
              pnode.withChildren(pnode.children.filterNot(_ eq p) ++ p.children)

            case _ => pnode
          }
      }
    )
}
