package org.grapheco.lynx.operator.utils

import org.grapheco.lynx.operator.{AggregationOperator, ExpandOperator, FilterOperator, JoinOperator, LimitOperator, NodeScanOperator, OrderByOperator, PathScanOperator, ProjectOperator, SelectOperator, SkipOperator}
import org.grapheco.lynx.physical.PPTExpandPath
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNumber
import org.grapheco.lynx.{ExecutionContext, ExecutionOperator, PPTAggregation, PPTFilter, PPTJoin, PPTLimit, PPTNode, PPTNodeScan, PPTOrderBy, PPTProject, PPTRelationshipScan, PPTSelect, PPTSkip, PhysicalPlannerContext}

/**
  *@description: This class is used to translate plan from OptimizedPlan to ExecutionOperator.
  */
class OperatorTranslator {
  def translator(
      plan: PPTNode,
      plannerContext: PhysicalPlannerContext,
      executionContext: ExecutionContext
    ): ExecutionOperator = {
    plan match {
      case PPTNodeScan(pattern) => {
        NodeScanOperator(
          pattern,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case PPTRelationshipScan(relPattern, leftPattern, rightPattern) => {
        PathScanOperator(
          relPattern,
          leftPattern,
          rightPattern,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case filter: PPTFilter => {
        FilterOperator(
          filter.expr,
          translator(filter.children.head, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case project: PPTProject => {
        val columnExpr = project.ri.items.map(x => x.name -> x.expression)
        ProjectOperator(
          translator(project.children.head, plannerContext, executionContext),
          columnExpr,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case select: PPTSelect => {
        SelectOperator(
          select.columns,
          translator(select.children.head, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case aggregation: PPTAggregation => {
        AggregationOperator(
          aggregation.aggregations,
          aggregation.groupings,
          translator(aggregation.in, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case expand: PPTExpandPath => {
        ExpandOperator(
          translator(expand.children.head, plannerContext, executionContext),
          expand.rel,
          expand.rightNode,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case join: PPTJoin => {
        // optimizer will choose small table.
        val smallTable = join.children.head
        val largeTable = join.children.last
        JoinOperator(
          translator(smallTable, plannerContext, executionContext),
          translator(largeTable, plannerContext, executionContext),
          join.filterExpr,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case limit: PPTLimit => {
        val limitNumber = plannerContext.runnerContext.expressionEvaluator
          .eval(limit.expr)(executionContext.expressionContext)
          .value
          .asInstanceOf[LynxNumber]
          .number
          .intValue()
        LimitOperator(
          limitNumber,
          translator(limit.children.head, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case skip: PPTSkip => {
        val skipNumber = plannerContext.runnerContext.expressionEvaluator
          .eval(skip.expr)(executionContext.expressionContext)
          .value
          .asInstanceOf[LynxNumber]
          .number
          .intValue()
        SkipOperator(
          skipNumber,
          translator(skip.children.head, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case orderBy: PPTOrderBy => {
        OrderByOperator(
          orderBy.sortItem,
          translator(orderBy.children.head, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
    }
  }
}
