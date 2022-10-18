package org.grapheco.lynx.expression

import org.opencypher.v9_0.expressions.{Expression, PathStep, SemanticDirection}

/**
  *@description:
  */
sealed trait LynxPathStep {
  def dependencies: Set[Expression]
}

final case class LynxNodePathStep(node: Expression, next: LynxPathStep) extends LynxPathStep {
  val dependencies = next.dependencies + node
}

final case class LynxSingleRelationshipPathStep(
    rel: Expression,
    direction: SemanticDirection,
    toNode: Option[LynxVariable],
    next: LynxPathStep)
  extends LynxPathStep {
  val dependencies = next.dependencies + rel
}

final case class LynxMultiRelationshipPathStep(
    rel: Expression,
    direction: SemanticDirection,
    toNode: Option[LynxVariable],
    next: LynxPathStep)
  extends LynxPathStep {
  val dependencies = next.dependencies + rel
}

case object LynxNilPathStep extends LynxPathStep {
  val dependencies = Set.empty[Expression]
}

case class LynxPathExpression(steps: LynxPathStep) extends LynxExpression {}
