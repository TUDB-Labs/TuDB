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

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  *@description: Filter operator is used to get data that contains a specific pattern.
  */
case class FilterOperator(
    in: ExecutionOperator,
    filterExpr: Expression,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var columnNames: Seq[String] = Seq.empty
  val outputRows: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer()

  override def openImpl(): Unit = {
    in.open()
    columnNames = in.outputSchema().map(nameAndType => nameAndType._1)
  }

  override def getNextImpl(): RowBatch = {
    while (outputRows.length < numRowsPerBatch) {
      val inputRows = in.getNext()
      if (inputRows.batchData.isEmpty) {
        if (outputRows.nonEmpty) {
          val remainingData = outputRows.toArray.toSeq
          outputRows.clear()
          return RowBatch(remainingData)
        } else return RowBatch(Seq.empty)
      }
      inputRows.batchData.foreach(inputRow => {
        expressionEvaluator.eval(filterExpr)(
          expressionContext.withVars(columnNames.zip(inputRow).toMap)
        ) match {
          case LynxBoolean(passFilter) => if (passFilter) outputRows.append(inputRow)
          case LynxNull                => {}
        }
      })
    }
    val output = RowBatch(outputRows.toArray.toSeq)
    outputRows.clear()
    output
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
