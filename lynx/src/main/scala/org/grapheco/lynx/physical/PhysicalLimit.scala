package org.grapheco.lynx.physical

import org.grapheco.lynx.planner.{ExecutionContext, PhysicalPlannerContext}
import org.grapheco.lynx.{DataFrame, LynxType}
import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class PhysicalLimit(
    expr: Expression
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    implicit val ec = ctx.expressionContext
    df.take(eval(expr).value.asInstanceOf[Number].intValue())
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalLimit =
    PhysicalLimit(expr)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema
}
