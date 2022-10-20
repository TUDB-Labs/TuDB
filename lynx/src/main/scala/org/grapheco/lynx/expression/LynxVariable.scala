package org.grapheco.lynx.expression

import org.opencypher.v9_0.expressions.LogicalVariable

/**
  *@description:
  */
// TODO: calculate columnOffset
case class LynxVariable(name: String) extends LogicalVariable with LynxExpression {
  override def copyId: LogicalVariable = LynxVariable(name)

  override def renameId(newName: String): LogicalVariable = LynxVariable(newName)

  override def bumpId: LogicalVariable = LynxVariable(name)
}
