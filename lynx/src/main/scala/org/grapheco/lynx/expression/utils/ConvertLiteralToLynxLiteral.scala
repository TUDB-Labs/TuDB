package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.{LynxBooleanLiteral, LynxDoubleLiteral, LynxIntegerLiteral, LynxLiteral, LynxNullLiteral, LynxStringLiteral}
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

      case True() => LynxBooleanLiteral(true)

      case False() => LynxBooleanLiteral(false)
    }
  }
}
