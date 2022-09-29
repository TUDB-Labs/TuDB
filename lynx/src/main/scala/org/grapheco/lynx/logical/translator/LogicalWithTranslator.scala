package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalCreateUnit, LogicalNode}
import org.opencypher.v9_0.ast.{Limit, ReturnItems, Skip, Where, With}

/**
  *@description:
  */
case class LogicalWithTranslator(w: With) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    (w, in) match {
      case (
          With(
            distinct,
            ReturnItems(includeExisting, items),
            orderBy,
            skip,
            limit: Option[Limit],
            where
          ),
          None
          ) =>
        LogicalCreateUnit(items)

      case (
          With(
            distinct,
            ri: ReturnItems,
            orderBy,
            skip: Option[Skip],
            limit: Option[Limit],
            where: Option[Where]
          ),
          Some(sin)
          ) =>
        PipedTranslators(
          Seq(
            LogicalProjectTranslator(ri),
            LogicalWhereTranslator(where),
            LogicalSkipTranslator(skip),
            LogicalOrderByTranslator(orderBy),
            LogicalLimitTranslator(limit),
            LogicalSelectTranslator(ri),
            LogicalDistinctTranslator(distinct)
          )
        ).translate(in)
    }
  }
}
