package org.grapheco.lynx.physical

import org.grapheco.lynx.planner.{ExecutionContext, PhysicalPlannerContext}
import org.grapheco.lynx.{DataFrame, LynxType}

/**
  *@description:
  */
case class PhysicalSelect(
    columns: Seq[(String, Option[String])]
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalSelect =
    PhysicalSelect(columns)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] =
    columns.map(x => x._2.getOrElse(x._1)).map(x => x -> in.schema.find(_._1 == x).get._2)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.select(columns)
  }
}
