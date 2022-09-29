package org.grapheco.lynx.physical

import org.grapheco.lynx.{DataFrame, LynxType}
import org.grapheco.lynx.planner.{ExecutionContext, PhysicalPlannerContext}

/**
  *@description:
  */
case class PhysicalDistinct()(implicit in: PhysicalNode, val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.distinct()
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalDistinct =
    PhysicalDistinct()(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema
}