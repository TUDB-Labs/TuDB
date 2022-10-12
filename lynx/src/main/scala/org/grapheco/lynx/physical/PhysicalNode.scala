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

package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{DataFrame, DataFrameOps, ExecutionContext, ExpressionContext, LynxType, TreeNode}
import org.opencypher.v9_0.ast.ReturnItem
import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
trait PhysicalNode extends TreeNode {
  override type SerialType = PhysicalNode
  override val children: Seq[PhysicalNode] = Seq.empty
  val schema: Seq[(String, LynxType)]

  def execute(implicit ctx: ExecutionContext): DataFrame

  def withChildren(children0: Seq[PhysicalNode]): PhysicalNode
}

trait AbstractPhysicalNode extends PhysicalNode {

  val plannerContext: PhysicalPlannerContext

  implicit def ops(ds: DataFrame): DataFrameOps =
    DataFrameOps(ds)(plannerContext.runnerContext.dataFrameOperator)

  val typeSystem = plannerContext.runnerContext.typeSystem
  val graphModel = plannerContext.runnerContext.graphModel
  val expressionEvaluator = plannerContext.runnerContext.expressionEvaluator
  val procedureRegistry = plannerContext.runnerContext.procedureRegistry

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue =
    expressionEvaluator.eval(expr)

  def typeOf(expr: Expression): LynxType =
    plannerContext.runnerContext.expressionEvaluator
      .typeOf(expr, plannerContext.parameterTypes.toMap)

  def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType =
    expressionEvaluator.typeOf(expr, definedVarTypes)

  def createUnitDataFrame(items: Seq[ReturnItem])(implicit ctx: ExecutionContext): DataFrame = {
    DataFrame.unit(items.map(item => item.name -> item.expression))(
      expressionEvaluator,
      ctx.expressionContext
    )
  }
}
