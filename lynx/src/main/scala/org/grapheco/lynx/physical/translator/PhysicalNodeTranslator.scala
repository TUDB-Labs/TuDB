package org.grapheco.lynx.physical.translator

import org.grapheco.lynx.planner.PhysicalPlannerContext
import org.grapheco.lynx.physical.PhysicalNode

/**
  *@description:
  */
trait PhysicalNodeTranslator {
  def translate(in: Option[PhysicalNode])(implicit ppc: PhysicalPlannerContext): PhysicalNode
}
