package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.{LynxDoubleLiteral, LynxFalse, LynxIntegerLiteral, LynxLiteral, LynxNullLiteral, LynxStringLiteral, LynxTrue}
import org.opencypher.v9_0.expressions.{DoubleLiteral, False, IntegerLiteral, Literal, Null, StringLiteral, True}

/**
  *@description:
  */
object ConvertLiteralToLynxLiteral {
  def convert(literal: Literal): LynxLiteral = {
    literal match {
      case i: IntegerLiteral => LynxIntegerLiteral(i.value)

      case d: DoubleLiteral => LynxDoubleLiteral(d.value)

      case s: StringLiteral => LynxStringLiteral(s.value)

      case Null() => LynxNullLiteral()

      case True() => LynxTrue()

      case False() => LynxFalse()
    }
  }
}
