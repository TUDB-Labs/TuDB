package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalLimit, LogicalNode}
import org.opencypher.v9_0.ast.Limit

/**
  *@description:
  */
case class LogicalLimitTranslator(limit: Option[Limit]) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    limit match {
      case None              => in.get
      case Some(Limit(expr)) => LogicalLimit(expr)(in.get)
    }
  }
}
