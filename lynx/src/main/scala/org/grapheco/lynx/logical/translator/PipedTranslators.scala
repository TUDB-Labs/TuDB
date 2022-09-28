package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.planner.LogicalPlannerContext
import org.grapheco.lynx.logical.LogicalNode

/**
  *@description: pipelines a set of LogicalNodes
  */
case class PipedTranslators(items: Seq[LogicalNodeTranslator]) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    items
      .foldLeft[Option[LogicalNode]](in) { (in, item) =>
        Some(item.translate(in)(plannerContext))
      }
      .get
  }
}
