package org.grapheco.lynx.expression.pattern

import org.grapheco.lynx.expression.{LynxExpression, LynxVariable}
import org.grapheco.lynx.types.structural.LynxRelationshipType
import org.opencypher.v9_0.expressions.SemanticDirection

case class LynxRelationshipPattern(
    variable: LynxVariable,
    types: Seq[LynxRelationshipType],
    lowerHop: Int,
    upperHop: Int,
    properties: Option[LynxExpression],
    direction: SemanticDirection) {}
