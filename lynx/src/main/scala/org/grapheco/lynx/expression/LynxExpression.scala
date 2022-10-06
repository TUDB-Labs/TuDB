package org.grapheco.lynx.expression

import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.InputPosition

/**
  *@description: transfer openCypher's expression to our lynxExpression.
  */
trait LynxExpression extends Expression {
  override def position: InputPosition = this.position

  override def productElement(n: Int): Any = this.productElement(n)

  override def productArity: Int = this.productArity

  override def canEqual(that: Any): Boolean = this.canEqual(that)

  // def typeInfo: LynxType
}
