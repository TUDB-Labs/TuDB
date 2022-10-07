package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.{LynxDecimalDoubleLiteral, LynxExpression, LynxFalse, LynxMapExpression, LynxNull, LynxSignedDecimalIntegerLiteral, LynxSignedHexIntegerLiteral, LynxSignedOctalIntegerLiteral, LynxStringLiteral, LynxTrue, LynxUnsignedDecimalIntegerLiteral}
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.opencypher.v9_0.expressions.{DecimalDoubleLiteral, Expression, False, MapExpression, Null, SignedDecimalIntegerLiteral, SignedHexIntegerLiteral, SignedOctalIntegerLiteral, StringLiteral, True, UnsignedDecimalIntegerLiteral}

/**
  *@description:
  */
object ConvertExpressionToLynxExpression {
  def convert(expr: Expression): LynxExpression = {
    expr match {
      case MapExpression(items) =>
        LynxMapExpression(items.map(kv => LynxPropertyKey(kv._1.name) -> convert(kv._2)))
      case SignedDecimalIntegerLiteral(stringVal)   => LynxSignedDecimalIntegerLiteral(stringVal)
      case UnsignedDecimalIntegerLiteral(stringVal) => LynxUnsignedDecimalIntegerLiteral(stringVal)
      case SignedOctalIntegerLiteral(stringVal)     => LynxSignedOctalIntegerLiteral(stringVal)
      case SignedHexIntegerLiteral(stringVal)       => LynxSignedHexIntegerLiteral(stringVal)
      case DecimalDoubleLiteral(stringVal)          => LynxDecimalDoubleLiteral(stringVal)
      case StringLiteral(value)                     => LynxStringLiteral(value)
      case Null()                                   => LynxNull()
      case True()                                   => LynxTrue()
      case False()                                  => LynxFalse()
    }
  }
}
