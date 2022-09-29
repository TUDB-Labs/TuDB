package org.grapheco.lynx.physical.translator

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical.{PhysicalNode, PhysicalSetClause}
import org.opencypher.v9_0.ast.SetItem

/**
  *@description:
  */
case class PhysicalSetClauseTranslator(setItems: Seq[SetItem]) extends PhysicalNodeTranslator {
  override def translate(
      in: Option[PhysicalNode]
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    PhysicalSetClause(setItems)(in.get, ppc)
  }
}
