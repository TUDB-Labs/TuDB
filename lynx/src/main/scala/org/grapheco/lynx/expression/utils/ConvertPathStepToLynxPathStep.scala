package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.{LynxMultiRelationshipPathStep, LynxNilPathStep, LynxNodePathStep, LynxPathStep, LynxSingleRelationshipPathStep, LynxVariable}
import org.opencypher.v9_0.expressions.{MultiRelationshipPathStep, NilPathStep, NodePathStep, PathStep, SingleRelationshipPathStep}

/**
  *@description:
  */
object ConvertPathStepToLynxPathStep {
  def convert(step: PathStep): LynxPathStep = {
    step match {
      case n: NodePathStep =>
        LynxNodePathStep(
          ConvertExpressionToLynxExpression.convert(n.node),
          convert(n.next)
        )

      case r: SingleRelationshipPathStep =>
        LynxSingleRelationshipPathStep(
          ConvertExpressionToLynxExpression.convert(r.rel),
          r.direction,
          r.toNode.map(logicalVariable => LynxVariable(logicalVariable.name)),
          convert(r.next)
        )

      case mr: MultiRelationshipPathStep =>
        LynxMultiRelationshipPathStep(
          ConvertExpressionToLynxExpression.convert(mr.rel),
          mr.direction,
          mr.toNode.map(logicalVariable => LynxVariable(logicalVariable.name)),
          convert(mr.next)
        )

      case NilPathStep => LynxNilPathStep
    }
  }
}
