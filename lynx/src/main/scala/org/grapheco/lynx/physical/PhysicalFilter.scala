package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class PhysicalFilter(
    expr: Expression
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val ec = ctx.expressionContext
    df.filter { (record: Seq[LynxValue]) =>
      eval(expr)(ec.withVars(df.schema.map(_._1).zip(record).toMap)) match {
        case LynxBoolean(b) => b
        case LynxNull       => false //todo check logic
      }
    }(ec)
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalFilter =
    PhysicalFilter(expr)(children0.head, plannerContext)
}
