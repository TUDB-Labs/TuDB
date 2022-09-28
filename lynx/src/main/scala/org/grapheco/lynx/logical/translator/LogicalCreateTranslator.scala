package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.{LogicalCreate, LogicalNode}
import org.grapheco.lynx.planner.LogicalPlannerContext
import org.opencypher.v9_0.ast.Create

/**
  *@description:
  */
case class LogicalCreateTranslator(c: Create) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
    LogicalCreate(c)(in)
}
