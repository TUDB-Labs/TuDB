package org.grapheco.lynx.expression

import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class LynxAdd(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression {}

case class LynxUnaryAdd(rhs: Expression)
  extends LynxExpression
  with LynxLeftUnaryOperatorExpression {}

case class LynxSubtract(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression {}

case class LynxUnarySubtract(rhs: Expression)
  extends LynxExpression
  with LynxLeftUnaryOperatorExpression {}

case class LynxMultiply(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression {}

case class LynxDivide(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression {}

case class LynxModulo(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression {}

case class LynxPow(lhs: Expression, rhs: Expression)
  extends LynxExpression
  with LynxBinaryOperatorExpression {}
