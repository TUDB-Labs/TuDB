package org.grapheco.lynx.expression

import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class LynxCaseExpression(
    expression: Option[Expression],
    alternatives: IndexedSeq[(Expression, Expression)],
    default: Option[Expression])
  extends LynxExpression {
  lazy val possibleExpressions: IndexedSeq[Expression] = alternatives.map(_._2) ++ default
}

object LynxCaseExpression {
  def apply(
      expression: Option[Expression],
      alternatives: List[(Expression, Expression)],
      default: Option[Expression]
    ): LynxCaseExpression =
    LynxCaseExpression(expression, alternatives.toIndexedSeq, default)
}
