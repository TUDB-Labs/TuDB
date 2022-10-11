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

package org.grapheco.lynx.execution

import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

/**
  *@author:John117
  *@createDate:2022/8/3
  *@description: Project Operator is used to choose the properties from node or relationship.
  */
case class ProjectOperator(
    in: ExecutionOperator,
    projectColumnExpr: Seq[(String, Expression)],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var projectSchema: Seq[(String, LynxType)] = Seq.empty
  var inColumnNames: Seq[String] = Seq.empty

  override def openImpl(): Unit = {
    in.open()
    projectSchema = projectColumnExpr.map {
      case (name, expression) =>
        name -> expressionEvaluator.typeOf(expression, in.outputSchema().toMap)
    }
    inColumnNames = in.outputSchema().map(os => os._1)
  }

  override def getNextImpl(): RowBatch = {
    val sourceData = in.getNext()
    val projectData = sourceData.batchData.map(rowData => {
      val recordCtx = expressionContext.withVars(inColumnNames.zip(rowData).toMap)
      projectColumnExpr.map(col => expressionEvaluator.eval(col._2)(recordCtx))
    })
    RowBatch(projectData)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = projectSchema
}
