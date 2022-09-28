package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.{LogicalFilter, LogicalNode}
import org.grapheco.lynx.planner.LogicalPlannerContext
import org.opencypher.v9_0.ast.Where

/**
  *@description:
  */
case class LogicalWhereTranslator(where: Option[Where]) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    where match {
      case None              => in.get
      case Some(Where(expr)) => LogicalFilter(expr)(in.get)
    }
  }
}
