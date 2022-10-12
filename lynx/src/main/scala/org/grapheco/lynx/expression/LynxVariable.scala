package org.grapheco.lynx.expression

import org.opencypher.v9_0.expressions.LogicalVariable

/**
  *@description:
  */
case class LynxVariable(name: String, columnOffset: Int)
  extends LogicalVariable
  with LynxExpression {
  override def copyId: LogicalVariable = LynxVariable(name, columnOffset)

  override def renameId(newName: String): LogicalVariable = LynxVariable(newName, columnOffset)

  override def bumpId: LogicalVariable = LynxVariable(name, columnOffset)
}
