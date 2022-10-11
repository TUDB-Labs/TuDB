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
import org.grapheco.lynx.types.property.LynxNumber
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
import org.opencypher.v9_0.expressions.Expression

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to limit return data. eg: [1,2,3,4], limit 2, then return [1, 2]
  */
case class LimitOperator(
    in: ExecutionOperator,
    limitDataSize: Int,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var isLimitDone: Boolean = false
  var countLimitNum: Int = 0

  override def openImpl(): Unit = {
    in.open()
  }

  override def getNextImpl(): RowBatch = {
    if (!isLimitDone) {
      val dataBatch = in.getNext()
      if (dataBatch.batchData.nonEmpty) {
        val currentLength = countLimitNum + dataBatch.batchData.length
        if (currentLength <= limitDataSize) {
          countLimitNum = currentLength
          dataBatch
        } else {
          isLimitDone = true
          val offset = limitDataSize - currentLength
          RowBatch(dataBatch.batchData.slice(0, offset))
        }
      } else {
        isLimitDone = true
        RowBatch(Seq.empty)
      }
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
