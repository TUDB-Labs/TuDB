package org.grapheco.lynx.physical.translator

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical.{PhysicalNode, PhysicalRemove}
import org.opencypher.v9_0.ast.RemoveItem

/**
  *@description:
  */
case class PhysicalRemoveTranslator(removeItems: Seq[RemoveItem]) extends PhysicalNodeTranslator {
  override def translate(
      in: Option[PhysicalNode]
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    PhysicalRemove(removeItems)(in.get, ppc)
  }
}
