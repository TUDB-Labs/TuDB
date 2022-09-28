package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.{LogicalNode, LogicalRemove}
import org.grapheco.lynx.planner.LogicalPlannerContext
import org.opencypher.v9_0.ast.Remove

/**
  *@description:
  */
case class LogicalRemoveTranslator(r: Remove) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
    LogicalRemove(r)(in)
}
