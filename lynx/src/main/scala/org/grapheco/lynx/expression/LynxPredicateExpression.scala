package org.grapheco.lynx.expression

import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class LynxAnd(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxAnds(exprs: Set[Expression]) extends LynxExpression with LynxMultiOperatorExpression

case class LynxOr(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxOrs(exprs: Set[Expression]) extends LynxExpression with LynxMultiOperatorExpression

case class LynxXor(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxNot(rhs: Expression) extends LynxExpression with LynxLeftUnaryOperatorExpression

case class LynxEquals(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxEquivalent(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxNotEquals(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxInvalidNotEquals(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxRegexMatch(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxIn(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxStartsWith(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxEndsWith(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxContains(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression

case class LynxIsNull(lhs: Expression) extends LynxExpression with LynxRightUnaryOperatorExpression

case class LynxIsNotNull(lhs: Expression)
  extends LynxExpression
  with LynxRightUnaryOperatorExpression

sealed trait LynxInequalityExpression extends LynxExpression with LynxBinaryOperatorExpression {
  def includeEquality: Boolean

  def negated: LynxInequalityExpression
  def swapped: LynxInequalityExpression

  def lhs: Expression
  def rhs: Expression
}

case class LynxLessThan(lhs: Expression, rhs: Expression) extends LynxInequalityExpression {
  override def includeEquality: Boolean = false

  override def negated: LynxInequalityExpression = LynxGreaterThanOrEqual(lhs, rhs)

  override def swapped: LynxInequalityExpression = LynxGreaterThan(rhs, lhs)
}

case class LynxLessThanOrEqual(lhs: Expression, rhs: Expression) extends LynxInequalityExpression {
  override def includeEquality: Boolean = true

  override def negated: LynxInequalityExpression = LynxGreaterThan(lhs, rhs)

  override def swapped: LynxInequalityExpression = LynxGreaterThanOrEqual(rhs, lhs)
}

case class LynxGreaterThan(lhs: Expression, rhs: Expression) extends LynxInequalityExpression {
  override def includeEquality: Boolean = false

  override def negated: LynxInequalityExpression = LynxLessThanOrEqual(lhs, rhs)

  override def swapped: LynxInequalityExpression = LynxLessThan(rhs, lhs)
}

case class LynxGreaterThanOrEqual(lhs: Expression, rhs: Expression)
  extends LynxInequalityExpression {
  override def includeEquality: Boolean = true

  override def negated: LynxInequalityExpression = LynxLessThan(lhs, rhs)

  override def swapped: LynxInequalityExpression = LynxLessThanOrEqual(rhs, lhs)
}
