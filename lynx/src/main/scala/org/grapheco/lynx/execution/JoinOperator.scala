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
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

/**
  *@description: This operator is used to join two operators by INNER-JOIN.
  */
case class JoinOperator(
    smallTable: ExecutionOperator,
    largeTable: ExecutionOperator,
    filterExpression: Seq[Expression],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(smallTable, largeTable)

  var joinedTableSchema: Seq[(String, LynxType)] = _
  var joinCols: Seq[String] = _
  var smallCols: Map[String, Int] = _
  var largeCols: Map[String, Int] = _

  var isInit: Boolean = false
  var cachedSmallTableMap: Map[Seq[LynxValue], Seq[Seq[LynxValue]]] = _
  var largeColsWithoutJoinCols: Map[String, Int] = _

  override def openImpl(): Unit = {
    smallTable.open()
    largeTable.open()
    smallCols = smallTable.outputSchema().map(nameAndType => nameAndType._1).zipWithIndex.toMap
    largeCols = largeTable.outputSchema().map(nameAndType => nameAndType._1).zipWithIndex.toMap
    joinCols = smallCols.keys.filter(col => largeCols.contains(col)).toSeq
    joinedTableSchema = smallTable
      .outputSchema() ++ largeTable.outputSchema().filter(x => !joinCols.contains(x._1))

    largeColsWithoutJoinCols = largeCols -- joinCols
  }

  override def getNextImpl(): RowBatch = {
    if (!isInit) {
      cachedSmallTableMap = OperatorUtils
        .getOperatorAllOutputs(smallTable)
        .flatMap(batch => batch.batchData)
        .map(row => {
          val joinColValue = joinCols.map(colName => row(smallCols(colName)))
          joinColValue -> row
        })
        .groupBy(valueRow => valueRow._1)
        .map(kv => kv._1 -> kv._2.map(f => f._2).toSeq) // joinCols --> rows
      isInit = true
    }

    var largeBatchData: Seq[Seq[LynxValue]] = Seq.empty
    var joinedRecords: Seq[Seq[LynxValue]] = Seq.empty
    do {
      largeBatchData = largeTable.getNext().batchData
      if (largeBatchData.isEmpty) return RowBatch(Seq.empty)
      joinedRecords = largeBatchData.flatMap(row => {
        val largeJoinedColeValue = joinCols.map(col => row(largeCols(col)))
        cachedSmallTableMap
          .getOrElse(largeJoinedColeValue, Seq.empty)
          .map(smallRow => {
            val largeRow =
              largeColsWithoutJoinCols.toSeq
                .map(colAndIndex => row(colAndIndex._2))
            smallRow ++ largeRow
          })
      })
      filterExpression.foreach(expr => {
        joinedRecords = joinedRecords.filter(row => {
          expressionEvaluator
            .eval(expr)(
              expressionContext.withVars(joinedTableSchema.map(f => f._1).zip(row).toMap)
            ) match {
            case LynxBoolean(v) => v
            case LynxNull       => false
          }
        })
      })
    } while (joinedRecords.isEmpty)

    RowBatch(joinedRecords)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = joinedTableSchema
}
