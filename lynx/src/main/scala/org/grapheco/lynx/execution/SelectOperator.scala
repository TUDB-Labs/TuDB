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
import org.opencypher.v9_0.util.symbols.CypherType

/**
  *@author:John117
  *@createDate:2022/8/3
  *@description:
  */
case class SelectOperator(
    in: ExecutionOperator,
    columnsToSelect: Seq[(String, Option[String])],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var inSchemaWithIndex: Map[String, (CypherType, Int)] = Map.empty
  var outPutSchema: Seq[(String, CypherType)] = Seq.empty

  override def openImpl(): Unit = {
    in.open()
    // columnName -> (CT-Type, index)
    inSchemaWithIndex = in.outputSchema().zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2)).toMap
    outPutSchema = columnsToSelect.map(column =>
      column._2.getOrElse(column._1) -> inSchemaWithIndex(column._1)._1
    )
  }

  override def getNextImpl(): RowBatch = {
    val data = in.getNext()
    val selectedBatchResult = data.batchData.map(rowData => {
      columnsToSelect.map(column => rowData.apply(inSchemaWithIndex(column._1)._2))
    })
    RowBatch(selectedBatchResult)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = outPutSchema
}
