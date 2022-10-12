package org.grapheco.lynx.expression

import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class LynxProperty(map: Expression, propertyKey: LynxPropertyKey) extends LynxExpression {}
