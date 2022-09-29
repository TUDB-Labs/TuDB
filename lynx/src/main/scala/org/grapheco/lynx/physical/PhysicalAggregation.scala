package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.opencypher.v9_0.ast.ReturnItem

/**
  *@description:
  */
case class PhysicalAggregation(
    aggregations: Seq[ReturnItem],
    groupings: Seq[ReturnItem]
  )(implicit val in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalAggregation =
    PhysicalAggregation(aggregations, groupings)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] =
    (groupings ++ aggregations).map(x => x.name -> x.expression).map { col =>
      col._1 -> typeOf(col._2, in.schema.toMap)
    }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.groupBy(
      groupings.map(x => x.name -> x.expression),
      aggregations.map(x => x.name -> x.expression)
    )(ctx.expressionContext)
  }
}
