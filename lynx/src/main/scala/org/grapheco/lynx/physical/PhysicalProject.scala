package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.opencypher.v9_0.ast.{ReturnItem, ReturnItems, ReturnItemsDef}

/**
  *@description:
  */
case class PhysicalProject(
    ri: ReturnItemsDef
  )(implicit val in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalProject =
    PhysicalProject(ri)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] =
    ri.items.map(x => x.name -> x.expression).map { col =>
      col._1 -> typeOf(col._2, in.schema.toMap)
    }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.project(ri.items.map(x => x.name -> x.expression))(ctx.expressionContext)
  }

  def withReturnItems(items: Seq[ReturnItem]) =
    PhysicalProject(ReturnItems(ri.includeExisting, items)(ri.position))(in, plannerContext)
}
