package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.{LogicalAggregation, LogicalCreateUnit, LogicalNode, LogicalProject}
import org.grapheco.lynx.planner.LogicalPlannerContext
import org.opencypher.v9_0.ast.ReturnItemsDef

/**
  *@description:
  */
case class LogicalProjectTranslator(ri: ReturnItemsDef) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    val newIn = in.getOrElse(LogicalCreateUnit(ri.items))
    if (ri.containsAggregate) {
      val (aggregatingItems, groupingItems) =
        ri.items.partition(i => i.expression.containsAggregate)
      LogicalAggregation(aggregatingItems, groupingItems)(newIn)
    } else {
      LogicalProject(ri)(newIn)
    }
  }
}
