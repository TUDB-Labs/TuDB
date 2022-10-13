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

import org.grapheco.lynx.execution._
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical._
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

  def translate(
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
      case expandPath: PhysicalExpandPath => {
        ExpandOperator(
          translate(expandPath.children.head, plannerContext, executionContext),
          expandPath.rel,
          expandPath.rightNode,
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
          translate(filter.children.head, plannerContext, executionContext),
          filter.expr,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case project: PhysicalProject => {
        val columnExpr = project.ri.items.map(x => x.name -> x.expression)
        ProjectOperator(
          translate(project.children.head, plannerContext, executionContext),
          columnExpr,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case select: PhysicalSelect => {
        SelectOperator(
          translate(select.children.head, plannerContext, executionContext),
          select.columns,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case aggregation: PhysicalAggregation => {
        AggregationOperator(
          translate(aggregation.in, plannerContext, executionContext),
          aggregation.aggregations,
          aggregation.groupings,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case expand: PhysicalExpandPath => {
        ExpandOperator(
          translate(expand.children.head, plannerContext, executionContext),
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
          translate(smallTable, plannerContext, executionContext),
          translate(largeTable, plannerContext, executionContext),
          join.filterExpr,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case limit: PhysicalLimit => {
        val limitNumber = plannerContext.runnerContext.expressionEvaluator
          .eval(limit.expr)(executionContext.expressionContext)
          .asInstanceOf[LynxNumber]
          .number
          .intValue()
        LimitOperator(
          translate(limit.children.head, plannerContext, executionContext),
          limitNumber,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case skip: PhysicalSkip => {
        val skipNumber = plannerContext.runnerContext.expressionEvaluator
          .eval(skip.expr)(executionContext.expressionContext)
          .asInstanceOf[LynxNumber]
          .number
          .intValue()
        SkipOperator(
          translate(skip.children.head, plannerContext, executionContext),
          skipNumber,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case orderBy: PhysicalOrderBy => {
        OrderByOperator(
          translate(orderBy.children.head, plannerContext, executionContext),
          orderBy.sortItem,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case create: PhysicalCreate => {
        val in = create.in.map(child => translate(child, plannerContext, executionContext))
        CreateOperator(
          in,
          create.schemaLocal,
          create.ops,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case createUnit: PhysicalCreateUnit => {
        val columnNames = createUnit.schema.map(nameAndType => nameAndType._1)
        val literalExpressions = createUnit.items.map(returnItem => returnItem.expression)
        LiteralOperator(
          columnNames,
          literalExpressions,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case delete: PhysicalDelete => {
        DeleteOperator(
          translate(delete.in, plannerContext, executionContext),
          plannerContext.runnerContext.graphModel,
          delete.delete.expressions,
          delete.delete.forced,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case setClause: PhysicalSetClause => {
        SetOperator(
          translate(setClause.in, plannerContext, executionContext),
          setClause.setItems,
          plannerContext.runnerContext.graphModel,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case remove: PhysicalRemove => {
        RemoveOperator(
          translate(remove.in, plannerContext, executionContext),
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
          translate(distinct.children.head, plannerContext, executionContext),
          Seq.empty,
          groupExpr,
          plannerContext.runnerContext.expressionEvaluator,
          executionContext.expressionContext
        )
      }
      case unwind: PhysicalUnwind => {
        if (unwind.in.isDefined) {
          UnwindOperator(
            translate(unwind.in.get, plannerContext, executionContext),
            Seq(unwind.variable.name),
            plannerContext.runnerContext.expressionEvaluator,
            executionContext.expressionContext
          )
        } else {
          LiteralOperator(
            Seq(unwind.variable.name),
            Seq(unwind.expression),
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
