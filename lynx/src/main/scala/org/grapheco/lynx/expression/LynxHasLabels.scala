package org.grapheco.lynx.expression

import org.grapheco.lynx.types.structural.LynxNodeLabel
import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class LynxHasLabels(expression: Expression, labels: Seq[LynxNodeLabel]) extends LynxExpression
