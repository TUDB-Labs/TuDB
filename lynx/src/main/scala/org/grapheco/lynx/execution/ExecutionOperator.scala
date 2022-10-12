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

package org.grapheco.lynx

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.expressions.Expression

/**
  * This interface is the parent of the rest of operators.
  * The execution flow for all the children operators are: open() --> getNext() --> close()
  */
trait ExecutionOperator extends TreeNode {
  override type SerialType = ExecutionOperator

  val numRowsPerBatch = 1

  // prepare for processing
  def open(): Unit = {
    openImpl()
  }
  // to be implemented by concrete operators
  def openImpl(): Unit

  // empty RowBatch means the end of output
  def getNext(): RowBatch = {
    getNextImpl()
  }
  // to be implemented by concrete operators
  def getNextImpl(): RowBatch

  def close(): Unit = {
    closeImpl()
  }
  // to be implemented by concrete operators
  def closeImpl(): Unit

  def outputSchema(): Seq[(String, LynxType)]
}

case class RowBatch(batchData: Seq[Seq[LynxValue]]) {}
