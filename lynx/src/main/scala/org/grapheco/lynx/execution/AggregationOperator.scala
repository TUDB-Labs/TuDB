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

import org.grapheco.lynx.execution.utils.OperatorUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.ast.ReturnItem
import org.opencypher.v9_0.expressions.Expression

/**
  *@description: This operator groups the input from `in` by keys of `aggregationItems`
  *                and evaluates aggregations by values of `aggregationItems`.
  */
case class AggregationOperator(
    in: ExecutionOperator,
    aggregationItems: Seq[(String, Expression)],
    groupingItems: Seq[(String, Expression)],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var schema: Seq[(String, LynxType)] = Seq.empty

  var allGroupedData: Iterator[Array[Seq[LynxValue]]] = Iterator.empty
  var hasPulledData: Boolean = false

  override def openImpl(): Unit = {
    in.open()
    schema = (aggregationItems ++ groupingItems).map(col =>
      col._1 -> expressionEvaluator.typeOf(col._2, in.outputSchema().toMap)
    )
  }

  override def getNextImpl(): RowBatch = {
    if (!hasPulledData) {
      val columnNames = in.outputSchema().map(f => f._1)
      val allData = OperatorUtils.getOperatorAllOutputs(in).flatMap(rowData => rowData.batchData)
      val result = if (groupingItems.nonEmpty) {
        allData
          .map(record => {
            val recordCtx = expressionContext.withVars(columnNames.zip(record).toMap)
            groupingItems.map(col => expressionEvaluator.eval(col._2)(recordCtx)) -> recordCtx
          })
          .toSeq // each row point to a recordCtx
          // group by record
          .groupBy(recordAndExprCtx => recordAndExprCtx._1)
          // each grouped record --> multiple recordCtx
          .mapValues(recordAndExprCtx => recordAndExprCtx.map(rc => rc._2))
          .map {
            case (groupingValue, recordCtx) =>
              groupingValue ++ {
                aggregationItems.map {
                  case (name, expr) =>
                    expressionEvaluator.aggregateEval(expr)(recordCtx)
                }
              }
          }
      } else {
        val allRecordContext = allData.map { record =>
          expressionContext.withVars(columnNames.zip(record).toMap)
        }.toSeq
        Iterator(aggregationItems.map {
          case (name, expression) =>
            expressionEvaluator.aggregateEval(expression)(allRecordContext)
        })
      }
      allGroupedData = result.toArray.grouped(numRowsPerBatch)
      hasPulledData = true
    }

    if (allGroupedData.nonEmpty) RowBatch(allGroupedData.next())
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema
}
