package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalNode, LogicalSelect}
import org.opencypher.v9_0.ast.ReturnItemsDef

/**
  *@description:
  */
object LogicalSelectTranslator {
  def apply(ri: ReturnItemsDef): LogicalSelectTranslator = LogicalSelectTranslator(
    ri.items.map(item => item.name -> item.alias.map(_.name))
  )
}

case class LogicalSelectTranslator(columns: Seq[(String, Option[String])])
  extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    LogicalSelect(columns)(in.get)
  }
}
