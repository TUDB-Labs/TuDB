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
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.util.symbols.CTAny

/**
  *@description: This operator is used to unwind list:
  *                 unwind [1,2,3,4] as x return x will get 4 rows
  *                    ==>     1
  *                            2
  *                            3
  *                            4
  *                  unwind [[1,2,3], [4,5,6]] as x will get 2 rows:
  *                    ==>
  *                        [1,2,3]
  *                        [4,5,6]
  */
case class UnwindOperator(
    in: ExecutionOperator,
    toUnwindColumnNames: Seq[String],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)
  var colNames: Seq[String] = _

  override def openImpl(): Unit = {
    in.open()
    colNames = in.outputSchema().map(nameAndType => nameAndType._1)
  }

  override def getNextImpl(): RowBatch = {
    // TODO: plan to guarantee toUnwindColumnNames in colNames.
    var batchData: Seq[Seq[LynxValue]] = Seq.empty
    var unwindResult: Seq[Seq[LynxValue]] = Seq.empty
    do {
      batchData = in.getNext().batchData
      if (batchData.isEmpty) return RowBatch(Seq.empty)
      unwindResult = batchData.flatMap(rowData => {
        val recordMap = colNames.zip(rowData).toMap
        val unwindValues = toUnwindColumnNames.map(name => {
          recordMap(name) match {
            case lst: LynxList      => lst.value
            case element: LynxValue => List(element)
          }
        })
        unwindValues
      })
    } while (unwindResult.isEmpty)

    RowBatch(unwindResult)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] =
    toUnwindColumnNames.map(name => (name, CTAny))
}
