package org.grapheco.lynx.expression.pattern

import LynxRelationshipPattern.{LowerHop, UpperHop}
import org.grapheco.lynx.expression.{LynxExpression, LynxVariable}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.opencypher.v9_0.expressions.SemanticDirection

object LynxRelationshipPattern {
  type LowerHop = Int
  type UpperHop = Int
}

case class LynxRelationshipPattern(
    variable: LynxVariable,
    types: Seq[String],
    length: (LowerHop, UpperHop),
    // TODO: properties should be lynxExpression
    properties: Map[LynxPropertyKey, LynxValue],
    direction: SemanticDirection) {}
