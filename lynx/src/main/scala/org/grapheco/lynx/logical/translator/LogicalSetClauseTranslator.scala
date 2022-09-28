package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.{LogicalNode, LogicalSetClause}
import org.grapheco.lynx.planner.LogicalPlannerContext
import org.opencypher.v9_0.ast.SetClause

/**
  *@description:
  */
case class LogicalSetClauseTranslator(s: SetClause) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
    LogicalSetClause(s)(in)
}
