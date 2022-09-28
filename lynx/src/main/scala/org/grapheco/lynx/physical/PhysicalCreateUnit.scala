package org.grapheco.lynx.physical

import org.grapheco.lynx.planner.{ExecutionContext, PhysicalPlannerContext}
import org.grapheco.lynx.{DataFrame, LynxType}
import org.opencypher.v9_0.ast.ReturnItem

/**
  *@description:
  */
case class PhysicalCreateUnit(items: Seq[ReturnItem])(val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalCreateUnit =
    PhysicalCreateUnit(items)(plannerContext)

  override val schema: Seq[(String, LynxType)] =
    items.map(item => item.name -> typeOf(item.expression))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    createUnitDataFrame(items)
  }
}
