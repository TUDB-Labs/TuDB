package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.planner.LogicalPlannerContext
import org.grapheco.lynx.logical.LogicalNode
import org.opencypher.v9_0.ast.Return

/**
  *@description:
  */
case class LogicalReturnTranslator(r: Return) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    val Return(distinct, ri, orderBy, skip, limit, excludedNames) = r

    PipedTranslators(
      Seq(
        LogicalProjectTranslator(ri),
        LogicalSkipTranslator(skip),
        LogicalLimitTranslator(limit),
        LogicalOrderByTranslator(orderBy),
        LogicalSelectTranslator(ri),
        LogicalDistinctTranslator(distinct)
      )
    ).translate(in)
  }
}
