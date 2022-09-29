package org.grapheco.lynx.planner

import org.grapheco.lynx.{CypherRunnerContext}
import org.grapheco.lynx.logical.{LogicalAggregation, LogicalCreate, LogicalCreateIndex, LogicalCreateUnit, LogicalDelete, LogicalDistinct, LogicalFilter, LogicalJoin, LogicalLimit, LogicalMerge, LogicalMergeAction, LogicalNode, LogicalOrderBy, LogicalPatternMatch, LogicalProcedureCall, LogicalProject, LogicalRemove, LogicalSelect, LogicalSetClause, LogicalSkip, LogicalUnwind}
import org.grapheco.lynx.physical.translator.{PhysicalCreateTranslator, PhysicalMergeTranslator, PhysicalPatternMatchTranslator, PhysicalRemoveTranslator, PhysicalSetClauseTranslator, PhysicalUnwindTranslator}
import org.grapheco.lynx.physical.{PhysicalAggregation, PhysicalCreateIndex, PhysicalCreateUnit, PhysicalDelete, PhysicalDistinct, PhysicalFilter, PhysicalJoin, PhysicalLimit, PhysicalMergeAction, PhysicalNode, PhysicalOrderBy, PhysicalProcedureCall, PhysicalProject, PhysicalSelect, PhysicalSkip}
import org.opencypher.v9_0.ast.{Create, Delete, Merge, MergeAction}
import org.opencypher.v9_0.expressions.{Expression, LabelName, Namespace, ProcedureName, PropertyKeyName}

/**
  *@description:
  */
class DefaultPhysicalPlanner(runnerContext: CypherRunnerContext) extends PhysicalPlanner {
  override def plan(
      logicalPlan: LogicalNode
    )(implicit plannerContext: PhysicalPlannerContext
    ): PhysicalNode = {
    implicit val runnerContext: CypherRunnerContext = plannerContext.runnerContext
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
      case ld @ LogicalDelete(d: Delete) => PhysicalDelete(d)(plan(ld.in), plannerContext)
      case ls @ LogicalSelect(columns: Seq[(String, Option[String])]) =>
        PhysicalSelect(columns)(plan(ls.in), plannerContext)
      case lp @ LogicalProject(ri)       => PhysicalProject(ri)(plan(lp.in), plannerContext)
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