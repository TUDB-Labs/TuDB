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

package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.LogicalNode
import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.opencypher.v9_0.ast.{Clause, Create, Delete, Match, Merge, QueryPart, Remove, Return, SetClause, SingleQuery, UnresolvedCall, Unwind, With}

/**
  *@description:
  */
case class LogicalQueryPartTranslator(part: QueryPart) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    part match {
      case SingleQuery(clauses: Seq[Clause]) =>
        PipedTranslators(
          clauses.map {
            case c: UnresolvedCall => LogicalProcedureCallTranslator(c)
            case r: Return         => LogicalReturnTranslator(r)
            case w: With           => LogicalWithTranslator(w)
            case u: Unwind         => LogicalUnwindTranslator(u)
            case m: Match          => LogicalMatchTranslator(m)
            case c: Create         => LogicalCreateTranslator(c)
            case m: Merge          => LogicalMergeTranslator(m)
            case d: Delete         => LogicalDeleteTranslator(d)
            case s: SetClause      => LogicalSetClauseTranslator(s)
            case r: Remove         => LogicalRemoveTranslator(r)
          }
        ).translate(in)
    }
  }
}
