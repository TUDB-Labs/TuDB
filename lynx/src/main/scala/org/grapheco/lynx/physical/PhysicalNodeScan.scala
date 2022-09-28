package org.grapheco.lynx.physical

import org.grapheco.lynx.planner.{ExecutionContext, PhysicalPlannerContext}
import org.grapheco.lynx.{DataFrame, LynxType, NodeFilter}
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, NodePattern}
import org.opencypher.v9_0.util.symbols.CTNode

/**
  *@description:
  */
case class PhysicalNodeScan(
    pattern: NodePattern
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalNodeScan =
    PhysicalNodeScan(pattern)(plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val NodePattern(
      Some(var0: LogicalVariable),
      labels: Seq[LabelName],
      properties: Option[Expression],
      baseNode: Option[LogicalVariable]
    ) = pattern
    Seq(var0.name -> CTNode)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val NodePattern(
      Some(var0: LogicalVariable),
      labels: Seq[LabelName],
      properties: Option[Expression],
      baseNode: Option[LogicalVariable]
    ) = pattern
    implicit val ec = ctx.expressionContext

    DataFrame(
      Seq(var0.name -> CTNode),
      () => {
        val nodes = if (labels.isEmpty) {
          graphModel.nodes(
            NodeFilter(
              Seq.empty,
              properties
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            )
          )
        } else
          graphModel.nodes(
            NodeFilter(
              labels.map(_.name).map(LynxNodeLabel),
              properties
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            )
          )

        nodes.map(Seq(_))
      }
    )
  }
}
