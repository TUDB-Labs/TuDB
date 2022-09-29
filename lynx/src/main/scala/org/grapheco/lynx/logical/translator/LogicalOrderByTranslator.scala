package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalNode, LogicalOrderBy}
import org.opencypher.v9_0.ast.OrderBy

/**
  *@description:
  */
case class LogicalOrderByTranslator(orderBy: Option[OrderBy]) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    orderBy match {
      case None        => in.get
      case Some(value) => LogicalOrderBy(value.sortItems)(in.get)
    }
  }
}
