package org.grapheco.lynx.expression

/**
  *@description:
  */
case class LynxListLiteral(expressions: Seq[LynxExpression]) extends LynxExpression {
  def map(f: LynxExpression => LynxExpression) = copy(expressions = expressions.map(f))

  override def asCanonicalStringVal: String =
    expressions.map(_.asCanonicalStringVal).mkString("[", ", ", "]")
}

case class ListSlice(list: LynxExpression, from: Option[LynxExpression], to: Option[LynxExpression])
  extends LynxExpression

case class ContainerIndex(expr: LynxExpression, idx: LynxExpression) extends LynxExpression
