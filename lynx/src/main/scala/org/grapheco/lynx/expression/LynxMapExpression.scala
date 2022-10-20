package org.grapheco.lynx.expression

import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class LynxMapExpression(items: Seq[(LynxPropertyKey, Expression)]) extends LynxExpression {}
