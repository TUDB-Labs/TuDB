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

package org.grapheco.lynx.logical.plan

import org.grapheco.lynx.logical.translator.LogicalQueryPartTranslator
import org.grapheco.lynx.logical.{LogicalCreateIndex, LogicalNode}
import org.grapheco.lynx.{CypherRunnerContext, UnknownASTNodeException}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{LabelName, Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.ASTNode

/**
  *@description:
  */
class DefaultLogicalPlanner(runnerContext: CypherRunnerContext) extends LogicalPlanner {
  private def translate(node: ASTNode)(implicit lpc: LogicalPlannerContext): LogicalNode = {
    node match {
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        LogicalQueryPartTranslator(part).translate(None)

      case CreateUniquePropertyConstraint(
          Variable(v1),
          LabelName(l),
          List(Property(Variable(v2), PropertyKeyName(p)))
          ) =>
        throw UnknownASTNodeException(node)

      case CreateIndex(labelName, properties) =>
        LogicalCreateIndex(labelName, properties)

      case _ =>
        throw UnknownASTNodeException(node)
    }
  }

  override def plan(statement: Statement, plannerContext: LogicalPlannerContext): LogicalNode = {
    translate(statement)(plannerContext)
  }
}
