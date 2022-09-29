package org.grapheco.lynx.execution

import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.symbols.CTAny

/**
  *@description: This operator is used to eval Literal expression.
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
