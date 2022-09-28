package org.grapheco.lynx.physical

import org.grapheco.lynx.planner.{ExecutionContext, PhysicalPlannerContext}
import org.grapheco.lynx.{DataFrame, LynxType}
import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName}
import org.opencypher.v9_0.util.symbols.CTAny

/**
  *@description:
  */
case class PhysicalCreateIndex(
    labelName: LabelName,
    properties: List[PropertyKeyName]
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    graphModel._helper.createIndex(labelName.name, properties.map(_.name).toSet)
    DataFrame.empty
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalNode = this

  override val schema: Seq[(String, LynxType)] = {
    Seq("CreateIndex" -> CTAny)
  }
}
