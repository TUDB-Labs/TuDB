package org.grapheco.lynx.expression

/**
  *@description:
  */
sealed trait LynxLiteral extends LynxExpression {
  def value: AnyRef
  def asCanonicalStringVal: String
}

sealed trait LynxNumberLiteral extends LynxLiteral {
  def stringVal: String
  override def asCanonicalStringVal: String = stringVal
}

sealed trait LynxIntegerLiteral extends LynxNumberLiteral {
  def value: java.lang.Long
}

sealed trait LynxSignedIntegerLiteral extends LynxIntegerLiteral
sealed trait LynxUnsignedIntegerLiteral extends LynxIntegerLiteral

sealed abstract class LynxDecimalIntegerLiteral(stringVal: String) extends LynxIntegerLiteral {
  lazy val value: java.lang.Long = java.lang.Long.parseLong(stringVal)
}

case class LynxSignedDecimalIntegerLiteral(stringVal: String)
  extends LynxDecimalIntegerLiteral(stringVal)
  with LynxSignedIntegerLiteral

case class LynxUnsignedDecimalIntegerLiteral(stringVal: String)
  extends LynxDecimalIntegerLiteral(stringVal)
  with LynxUnsignedIntegerLiteral

sealed abstract class LynxOctalIntegerLiteral(stringVal: String) extends LynxIntegerLiteral {
  lazy val value: java.lang.Long = java.lang.Long.parseLong(stringVal, 8)
}

case class LynxSignedOctalIntegerLiteral(stringVal: String)
  extends LynxOctalIntegerLiteral(stringVal)
  with LynxSignedIntegerLiteral

sealed abstract class LynxHexIntegerLiteral(stringVal: String) extends LynxIntegerLiteral {
  lazy val value: java.lang.Long =
    if (stringVal.charAt(0) == '-')
      -java.lang.Long.parseLong(stringVal.substring(3), 16)
    else
      java.lang.Long.parseLong(stringVal.substring(2), 16)
}

case class LynxSignedHexIntegerLiteral(stringVal: String)
  extends LynxHexIntegerLiteral(stringVal)
  with LynxSignedIntegerLiteral

sealed trait LynxDoubleLiteral extends LynxNumberLiteral {
  def value: java.lang.Double
}

case class LynxDecimalDoubleLiteral(stringVal: String) extends LynxDoubleLiteral {
  lazy val value: java.lang.Double = java.lang.Double.parseDouble(stringVal)
}

case class LynxStringLiteral(value: String) extends LynxLiteral {
  override def asCanonicalStringVal = value
}

case class LynxNull() extends LynxLiteral {
  val value = null

  override def asCanonicalStringVal = "NULL"
}

sealed trait LynxBooleanLiteral extends LynxLiteral

case class LynxTrue() extends LynxBooleanLiteral {
  val value: java.lang.Boolean = true

  override def asCanonicalStringVal = "true"
}

case class LynxFalse() extends LynxBooleanLiteral {
  val value: java.lang.Boolean = false

  override def asCanonicalStringVal = "false"
}
