package org.grapheco.lynx.expression

/**
  *@description:
  */
sealed trait LynxLiteral extends LynxExpression {
  def value: AnyRef
}

sealed trait LynxNumberLiteral extends LynxLiteral

case class LynxIntegerLiteral(value: java.lang.Long) extends LynxNumberLiteral

case class LynxDoubleLiteral(value: java.lang.Double) extends LynxNumberLiteral

case class LynxStringLiteral(value: String) extends LynxLiteral

case class LynxNullLiteral() extends LynxLiteral {
  val value = null
}

case class LynxBooleanLiteral(value: java.lang.Boolean) extends LynxLiteral
