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
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.ast.{AscSortItem, DescSortItem, SortItem}
import org.opencypher.v9_0.expressions.Expression

/**
  *@author:John117
  *@createDate:2022/8/4
  *@description: This operator is used to sort data by specified expressions.
  */
case class OrderByOperator(
    in: ExecutionOperator,
    sortItem: Seq[SortItem],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var sortItems: Seq[(Expression, OrderByType)] = sortItem.map {
    case AscSortItem(expression)  => (expression, OrderByType.ASC)
    case DescSortItem(expression) => (expression, OrderByType.DESC)
  }

  var allGroupedSortedData: Iterator[Array[Seq[LynxValue]]] = Iterator.empty
  var hasPulledData: Boolean = false

  override def openImpl(): Unit = {
    in.open()
  }

  override def getNextImpl(): RowBatch = {
    if (!hasPulledData) {
      val allData = OperatorUtils.getOperatorAllOutputs(in).flatMap(rowData => rowData.batchData)
      val schemaName = in.outputSchema().map(x => x._1)
      allGroupedSortedData = allData
        .sortWith((a, b) => sortByItem(a, b, sortItems, schemaName)) // maybe it's a bad sort method.
        .grouped(numRowsPerBatch)
      hasPulledData = true
    }
    if (allGroupedSortedData.nonEmpty) RowBatch(allGroupedSortedData.next())
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()

  private def sortByItem(
      a: Seq[LynxValue],
      b: Seq[LynxValue],
      items: Seq[(Expression, OrderByType)],
      schema: Seq[String]
    ): Boolean = {
    // [true,true] means current expression cannot sort the two data.
    // The first true/false means the compare result(.sortWith is a asc method, true means the two data is asc).
    // The second true/false determines whether the next expression is required. (true means require).

    val comparedResult = items.foldLeft((true, true)) { (result, item) =>
      {
        result match {
          case (true, true) => { // means previous expression cannot compare two value
            val evaluatedValue1 = expressionEvaluator.eval(item._1)(
              expressionContext.withVars(schema.zip(a).toMap)
            )
            val evaluatedValue2 = expressionEvaluator.eval(item._1)(
              expressionContext.withVars(schema.zip(b).toMap)
            )
            item._2 match {
              // LynxNull = MAX
              case OrderByType.ASC => {
                if (evaluatedValue1 == LynxNull && evaluatedValue2 != LynxNull) (false, false)
                else if (evaluatedValue1 == LynxNull && evaluatedValue2 == LynxNull) (true, true)
                else if (evaluatedValue1 != LynxNull && evaluatedValue2 == LynxNull) (true, false)
                else (evaluatedValue1 <= evaluatedValue2, evaluatedValue1 == evaluatedValue2)
              }
              case OrderByType.DESC => {
                if (evaluatedValue1 == LynxNull && evaluatedValue2 != LynxNull) (true, false)
                else if (evaluatedValue1 == LynxNull && evaluatedValue2 == LynxNull) (true, true)
                else if (evaluatedValue1 != LynxNull && evaluatedValue2 == LynxNull) (false, false)
                else (evaluatedValue1 >= evaluatedValue2, evaluatedValue1 == evaluatedValue2)
              }
            }
          }
          case _ => result
        }
      }
    }
    comparedResult._1
  }
}

trait OrderByType {}

case object OrderByType {
  case object ASC extends OrderByType
  case object DESC extends OrderByType
}
