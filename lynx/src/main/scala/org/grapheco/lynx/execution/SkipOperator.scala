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
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to skip data. eg: [1,2,3,4], skip 2, then return [3,4]
  */
case class SkipOperator(
    in: ExecutionOperator,
    skipDataSize: Int,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  val outputRows: ArrayBuffer[Seq[LynxValue]] = ArrayBuffer.empty
  var isSkipDone: Boolean = false
  override def openImpl(): Unit = {
    in.open()
  }

  override def getNextImpl(): RowBatch = {
    if (!isSkipDone) {
      var skippedDataSize: Int = 0
      var dataBatch = in.getNext().batchData
      var returnData: Seq[Seq[LynxValue]] = Seq.empty
      while (dataBatch.nonEmpty && skippedDataSize <= skipDataSize) {
        val length = dataBatch.length
        val tmpLength = skippedDataSize + length
        if (tmpLength <= skipDataSize) dataBatch = in.getNext().batchData
        else {
          val offset = skipDataSize - skippedDataSize
          returnData = dataBatch.slice(offset, length)
        }
        skippedDataSize = tmpLength
      }
      isSkipDone = true
      RowBatch(returnData)
    } else in.getNext()
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
