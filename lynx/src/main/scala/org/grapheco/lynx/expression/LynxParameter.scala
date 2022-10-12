package org.grapheco.lynx.expression

import org.opencypher.v9_0.util.symbols.CypherType

/**
  *@description:
  */
case class LynxParameter(name: String, parameterType: CypherType) extends LynxExpression {}
