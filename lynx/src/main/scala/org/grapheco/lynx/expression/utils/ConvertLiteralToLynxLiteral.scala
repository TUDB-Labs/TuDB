package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.{LynxDecimalDoubleLiteral, LynxExpression, LynxFalse, LynxLiteral, LynxNullLiteral, LynxSignedDecimalIntegerLiteral, LynxSignedHexIntegerLiteral, LynxSignedOctalIntegerLiteral, LynxStringLiteral, LynxTrue, LynxUnsignedDecimalIntegerLiteral}
import org.opencypher.v9_0.expressions.{DecimalDoubleLiteral, False, Literal, Null, SignedDecimalIntegerLiteral, SignedHexIntegerLiteral, SignedOctalIntegerLiteral, StringLiteral, True, UnsignedDecimalIntegerLiteral}

/**
  *@description:
  */
object ConvertLiteralToLynxLiteral {
  def convert(literal: Literal): LynxLiteral = {
    literal match {
      case SignedDecimalIntegerLiteral(stringVal)   => LynxSignedDecimalIntegerLiteral(stringVal)
      case UnsignedDecimalIntegerLiteral(stringVal) => LynxUnsignedDecimalIntegerLiteral(stringVal)
      case SignedOctalIntegerLiteral(stringVal)     => LynxSignedOctalIntegerLiteral(stringVal)
      case SignedHexIntegerLiteral(stringVal)       => LynxSignedHexIntegerLiteral(stringVal)
      case DecimalDoubleLiteral(stringVal)          => LynxDecimalDoubleLiteral(stringVal)
      case StringLiteral(value)                     => LynxStringLiteral(value)
      case Null()                                   => LynxNullLiteral()
      case True()                                   => LynxTrue()
      case False()                                  => LynxFalse()
    }
  }
}
