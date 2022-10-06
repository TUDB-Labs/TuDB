package org.grapheco.lynx.expression.pattern

import org.grapheco.lynx.expression.{LynxExpression, LynxVariable}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}

case class LynxNodePattern(
    variable: LynxVariable,
    labels: Seq[LynxNodeLabel],
    // TODO: properties should be lynxExpression
    properties: Map[LynxPropertyKey, LynxValue]) {}
