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
import org.opencypher.v9_0.util.symbols.CTAny

/**
  *@description: This operator is used to evaluate single Literal-only row. There can be one or more Literal columns.
  */
case class LiteralOperator(
    colNames: Seq[String],
    literalExpressions: Seq[Expression],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  var isEvalDone: Boolean = false

  override def openImpl(): Unit = {}

  override def getNextImpl(): RowBatch = {
    if (!isEvalDone) {
      val literals =
        literalExpressions.map(expr => expressionEvaluator.eval(expr)(expressionContext))

      isEvalDone = true
      RowBatch(Seq(literals))
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = colNames.map(name => (name, CTAny))
}
