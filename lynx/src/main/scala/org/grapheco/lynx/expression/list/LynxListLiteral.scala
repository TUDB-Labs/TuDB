package org.grapheco.lynx.expression.list

import org.grapheco.lynx.expression.LynxExpression
import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class LynxListLiteral(expressions: Seq[Expression]) extends LynxExpression

case class LynxListSlice(list: Expression, from: Option[Expression], to: Option[Expression])
  extends LynxExpression

case class LynxContainerIndex(expr: Expression, idx: Expression) extends LynxExpression
