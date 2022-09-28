package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.planner.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalMerge, LogicalMergeAction, LogicalNode}
import org.opencypher.v9_0.ast.{Match, Merge}

/**
  *@description:
  */
case class LogicalMergeTranslator(m: Merge) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    val matchInfo = Match(false, m.pattern, Seq.empty, m.where)(m.position)
    val mergeIn = LogicalMatchTranslator(matchInfo).translate(in)
    val mergeInfo = LogicalMerge(m)(Option(mergeIn))

    if (m.actions.nonEmpty) LogicalMergeAction(m.actions)(Option(mergeInfo))
    else mergeInfo
  }
}
