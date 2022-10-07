package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.{LynxExpression, LynxMapExpression}
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.opencypher.v9_0.expressions.{Expression, MapExpression}

/**
  *@description:
  */
object ConvertExpressionToLynxExpression {
  def convert(expr: Expression): LynxExpression = {
    expr match {
      case MapExpression(items) =>
        LynxMapExpression(items.map(kv => LynxPropertyKey(kv._1.name) -> convert(kv._2)))

    }
  }
}
