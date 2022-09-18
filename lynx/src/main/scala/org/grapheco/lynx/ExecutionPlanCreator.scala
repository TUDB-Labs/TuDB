package org.grapheco.lynx

import org.grapheco.lynx.operator._
import org.grapheco.lynx.physical.PhysicalExpandPath
import org.grapheco.lynx.types.property.LynxNumber
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
import org.opencypher.v9_0.ast.AliasedReturnItem
import org.opencypher.v9_0.expressions.Variable
import org.opencypher.v9_0.util.InputPosition

/**
  *@description: This class is used to translate physical plan to execution plan.
  */
class ExecutionPlanCreator {
  val defaultPosition = InputPosition(0, 0, 0)
  def translator(
      plan: PhysicalNode,
      plannerContext: PhysicalPlannerContext,
      executionContext: ExecutionContext
    ): ExecutionOperator = {
    plan match {
      case PhysicalNodeScan(pattern) => {
        NodeScanOperator(
          pattern,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case PhysicalRelationshipScan(relPattern, leftPattern, rightPattern) => {
        PathScanOperator(
          relPattern,
          leftPattern,
          rightPattern,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case filter: PhysicalFilter => {
        FilterOperator(
          filter.expr,
          translator(filter.children.head, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case project: PhysicalProject => {
        val columnExpr = project.ri.items.map(x => x.name -> x.expression)
        ProjectOperator(
          translator(project.children.head, plannerContext, executionContext),
          columnExpr,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case select: PhysicalSelect => {
        SelectOperator(
          select.columns,
          translator(select.children.head, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case aggregation: PhysicalAggregation => {
        AggregationOperator(
          aggregation.aggregations,
          aggregation.groupings,
          translator(aggregation.in, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case expand: PhysicalExpandPath => {
        ExpandOperator(
          translator(expand.children.head, plannerContext, executionContext),
          expand.rel,
          expand.rightNode,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case join: PhysicalJoin => {
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
      case limit: PhysicalLimit => {
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
      case skip: PhysicalSkip => {
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
      case orderBy: PhysicalOrderBy => {
        OrderByOperator(
          orderBy.sortItem,
          translator(orderBy.children.head, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case create: PhysicalCreate => {
        val in = create.in.map(child => translator(child, plannerContext, executionContext))
        CreateOperator(
          in,
          create.schemaLocal,
          create.ops,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case delete: PhysicalDelete => {
        DeleteOperator(
          translator(delete.in, plannerContext, executionContext),
          plannerContext.runnerContext.graphModel,
          delete.delete.forced,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case remove: PhysicalRemove => {
        RemoveOperator(
          translator(remove.in, plannerContext, executionContext),
          remove.removeItems,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case distinct: PhysicalDistinct => {
        val groupExpr = distinct.schema.map(nameAndType =>
          AliasedReturnItem(
            Variable(nameAndType._1)(defaultPosition),
            Variable(nameAndType._1)(defaultPosition)
          )(defaultPosition)
        )
        AggregationOperator(
          Seq.empty,
          groupExpr,
          translator(distinct.children.head, plannerContext, executionContext),
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case unwind: PhysicalUnwind => {
        if (unwind.in.isDefined) {
          UnwindOperator(
            translator(unwind.in.get, plannerContext, executionContext),
            unwind.variable.name,
            plannerContext.runnerContext.expressionEvaluator,
            executionContext.expressionContext
          )
        } else {
          LiteralOperator(
            unwind.variable.name,
            unwind.expression,
            plannerContext.runnerContext.expressionEvaluator,
            executionContext.expressionContext
          )
        }
      }

      case unsupportedPlan => {
        throw new TuDBException(
          TuDBError.LYNX_UNSUPPORTED_OPERATION,
          s"unsupported physical plan: ${unsupportedPlan.toString}"
        )
      }
    }
  }
}
