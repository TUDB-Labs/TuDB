package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.planner.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalDistinct, LogicalNode}

/**
  *@description:
  */
case class LogicalDistinctTranslator(distinct: Boolean) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    distinct match {
      case false => in.get
      case true  => LogicalDistinct()(in.get)
    }
  }
}
