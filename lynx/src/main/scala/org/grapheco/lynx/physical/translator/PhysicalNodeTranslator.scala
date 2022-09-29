package org.grapheco.lynx.physical.translator

import org.grapheco.lynx.physical.PhysicalNode
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext

/**
  *@description:
  */
trait PhysicalNodeTranslator {
  def translate(in: Option[PhysicalNode])(implicit ppc: PhysicalPlannerContext): PhysicalNode
}
