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

package org.grapheco.lynx.physical.plan

import org.grapheco.lynx.QueryRunnerContext
import org.grapheco.lynx.logical._
import org.grapheco.lynx.physical.translator._
import org.grapheco.lynx.physical._
import org.opencypher.v9_0.ast.{Create, Delete, Merge, MergeAction}
import org.opencypher.v9_0.expressions._

/**
  *@description:
  */
class DefaultPhysicalPlanner(runnerContext: QueryRunnerContext) extends PhysicalPlanner {
  override def plan(
      logicalPlan: LogicalNode
    )(implicit plannerContext: PhysicalPlannerContext
    ): PhysicalNode = {
    implicit val runnerContext: QueryRunnerContext = plannerContext.runnerContext
    logicalPlan match {
      case LogicalProcedureCall(
          procedureNamespace: Namespace,
          procedureName: ProcedureName,
          declaredArguments: Option[Seq[Expression]]
          ) =>
        PhysicalProcedureCall(
          procedureNamespace: Namespace,
          procedureName: ProcedureName,
          declaredArguments: Option[Seq[Expression]]
        )
      case lc @ LogicalCreate(c: Create) =>
        PhysicalCreateTranslator(c).translate(lc.in.map(plan(_)))(plannerContext)
      case lm @ LogicalMerge(m: Merge) =>
        PhysicalMergeTranslator(m).translate(lm.in.map(plan(_)))(plannerContext)
      case lm @ LogicalMergeAction(m: Seq[MergeAction]) =>
        PhysicalMergeAction(m)(plan(lm.in.get), plannerContext)
      case ld @ LogicalDelete(deleteExpr: Seq[Expression], forceToDelete: Boolean) =>
        PhysicalDelete(deleteExpr, forceToDelete)(plan(ld.in), plannerContext)
      case ls @ LogicalSelect(columns: Seq[(String, Option[String])]) =>
        PhysicalSelect(columns)(plan(ls.in), plannerContext)
      case lp @ LogicalProject(projectItems) =>
        PhysicalProject(projectItems)(plan(lp.in), plannerContext)
      case la @ LogicalAggregation(a, g) => PhysicalAggregation(a, g)(plan(la.in), plannerContext)
      case lc @ LogicalCreateUnit(items) => PhysicalCreateUnit(items)(plannerContext)
      case lf @ LogicalFilter(expr)      => PhysicalFilter(expr)(plan(lf.in), plannerContext)
      case ld @ LogicalDistinct()        => PhysicalDistinct()(plan(ld.in), plannerContext)
      case ll @ LogicalLimit(expr)       => PhysicalLimit(expr)(plan(ll.in), plannerContext)
      case lo @ LogicalOrderBy(sortItem) => PhysicalOrderBy(sortItem)(plan(lo.in), plannerContext)
      case ll @ LogicalSkip(expr)        => PhysicalSkip(expr)(plan(ll.in), plannerContext)
      case lj @ LogicalJoin(isSingleMatch) =>
        PhysicalJoin(Seq.empty, isSingleMatch)(plan(lj.a), plan(lj.b), plannerContext)
      case patternMatch: LogicalPatternMatch =>
        PhysicalPatternMatchTranslator(patternMatch)(plannerContext).translate(None)
      case li @ LogicalCreateIndex(labelName: LabelName, properties: List[PropertyKeyName]) =>
        PhysicalCreateIndex(labelName, properties)(plannerContext)
      case sc @ LogicalSetClause(d) =>
        PhysicalSetClauseTranslator(d.items).translate(sc.in.map(plan(_)))(plannerContext)
      case lr @ LogicalRemove(r) =>
        PhysicalRemoveTranslator(r.items).translate(lr.in.map(plan(_)))(plannerContext)
      case lu @ LogicalUnwind(u) =>
        PhysicalUnwindTranslator(u.expression, u.variable)
          .translate(lu.in.map(plan(_)))(plannerContext)
    }
  }
}
