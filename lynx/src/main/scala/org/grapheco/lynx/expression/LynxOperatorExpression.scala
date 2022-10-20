package org.grapheco.lynx.expression

import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
trait LynxOperatorExpression

trait LynxLeftUnaryOperatorExpression extends LynxOperatorExpression {
  def rhs: Expression
}

trait LynxRightUnaryOperatorExpression extends LynxOperatorExpression {
  def lhs: Expression
}

trait LynxBinaryOperatorExpression extends LynxOperatorExpression {
  def lhs: Expression
  def rhs: Expression
}

trait LynxMultiOperatorExpression extends LynxOperatorExpression {
  def exprs: Set[Expression]
}
