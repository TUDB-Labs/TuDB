package org.grapheco.lynx.expression

/**
  *@description:
  */
case class LynxListLiteral(expressions: Seq[LynxExpression]) extends LynxExpression

case class LynxListSlice(
    list: LynxExpression,
    from: Option[LynxExpression],
    to: Option[LynxExpression])
  extends LynxExpression

case class LynxContainerIndex(expr: LynxExpression, idx: LynxExpression) extends LynxExpression
