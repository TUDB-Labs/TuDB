package org.grapheco.lynx.physical.translator

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical.{PhysicalNode, PhysicalUnwind}
import org.opencypher.v9_0.expressions.{Expression, Variable}

/**
  *@description:
  */
case class PhysicalUnwindTranslator(expression: Expression, variable: Variable)
  extends PhysicalNodeTranslator {
  override def translate(
      in: Option[PhysicalNode]
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    PhysicalUnwind(expression, variable)(in, ppc)
  }
}
