package org.grapheco.lynx.operator

import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.symbols.CTAny

/**
  *@description: This operator is used to eval Literal expression.
  */
case class LiteralOperator(
    schemaName: String,
    literalExpression: Expression,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  var isEvalDone: Boolean = false

  override def openImpl(): Unit = {}

  override def getNextImpl(): RowBatch = {
    if (!isEvalDone) {
      val literal = expressionEvaluator.eval(literalExpression)(expressionContext)
      isEvalDone = true
      RowBatch(Seq(Seq(literal)))
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = Seq((schemaName, CTAny))
}
