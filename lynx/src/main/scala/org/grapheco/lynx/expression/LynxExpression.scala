package org.grapheco.lynx.expression

import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.InputPosition

/**
  *@description: In the future we will not extends Expression.
  */
trait LynxExpression extends Expression {
  override def position: InputPosition = InputPosition(0, 0, 0)
}
