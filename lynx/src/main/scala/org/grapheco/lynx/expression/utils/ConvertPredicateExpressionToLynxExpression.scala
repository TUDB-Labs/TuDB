package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.{LynxAdd, LynxAnd, LynxAnds, LynxContains, LynxDivide, LynxEndsWith, LynxEquals, LynxEquivalent, LynxExpression, LynxGreaterThan, LynxGreaterThanOrEqual, LynxIn, LynxInvalidNotEquals, LynxIsNotNull, LynxIsNull, LynxLessThan, LynxLessThanOrEqual, LynxModulo, LynxMultiply, LynxNot, LynxNotEquals, LynxOr, LynxOrs, LynxPow, LynxRegexMatch, LynxStartsWith, LynxSubtract, LynxUnaryAdd, LynxUnarySubtract, LynxXor}
import org.opencypher.v9_0.expressions.{Add, And, Ands, Contains, Divide, EndsWith, Equals, Equivalent, Expression, GreaterThan, GreaterThanOrEqual, In, InvalidNotEquals, IsNotNull, IsNull, LessThan, LessThanOrEqual, Modulo, Multiply, Not, NotEquals, Or, Ors, Pow, RegexMatch, StartsWith, Subtract, UnaryAdd, UnarySubtract, Xor}

/**
  *@description:
  */
object ConvertPredicateExpressionToLynxExpression {
  def convert(expr: Expression): LynxExpression = {
    expr match {
      case And(lhs, rhs) =>
        LynxAnd(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case Ands(exprs) =>
        LynxAnds(exprs.map(expr => ConvertExpressionToLynxExpression.convert(expr)))

      case Or(lhs, rhs) =>
        LynxOr(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case Ors(exprs) => LynxOrs(exprs.map(expr => ConvertExpressionToLynxExpression.convert(expr)))

      case Xor(lhs, rhs) =>
        LynxXor(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case Not(rhs) => LynxNot(ConvertExpressionToLynxExpression.convert(rhs))

      case Equals(lhs, rhs) =>
        LynxEquals(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case Equivalent(lhs, rhs) =>
        LynxEquivalent(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case NotEquals(lhs, rhs) =>
        LynxNotEquals(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case InvalidNotEquals(lhs, rhs) =>
        LynxInvalidNotEquals(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case RegexMatch(lhs, rhs) =>
        LynxRegexMatch(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case In(lhs, rhs) =>
        LynxIn(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case StartsWith(lhs, rhs) =>
        LynxStartsWith(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case EndsWith(lhs, rhs) =>
        LynxEndsWith(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case Contains(lhs, rhs) =>
        LynxContains(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case IsNull(lhs) => LynxIsNull(ConvertExpressionToLynxExpression.convert(lhs))

      case IsNotNull(lhs) => LynxIsNotNull(ConvertExpressionToLynxExpression.convert(lhs))

      case LessThan(lhs, rhs) =>
        LynxLessThan(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case LessThanOrEqual(lhs, rhs) =>
        LynxLessThanOrEqual(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case GreaterThan(lhs, rhs) =>
        LynxGreaterThan(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case GreaterThanOrEqual(lhs, rhs) =>
        LynxGreaterThanOrEqual(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case Add(lhs, rhs) =>
        LynxAdd(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case UnaryAdd(rhs) => LynxUnaryAdd(ConvertExpressionToLynxExpression.convert(rhs))

      case Subtract(lhs, rhs) =>
        LynxSubtract(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case UnarySubtract(rhs) => LynxUnarySubtract(ConvertExpressionToLynxExpression.convert(rhs))

      case Multiply(lhs, rhs) =>
        LynxMultiply(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case Divide(lhs, rhs) =>
        LynxDivide(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case Modulo(lhs, rhs) =>
        LynxModulo(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )

      case Pow(lhs, rhs) =>
        LynxPow(
          ConvertExpressionToLynxExpression.convert(lhs),
          ConvertExpressionToLynxExpression.convert(rhs)
        )
    }
  }
}
