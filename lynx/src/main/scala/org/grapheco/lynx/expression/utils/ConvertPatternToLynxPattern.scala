package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.LynxVariable
import org.grapheco.lynx.expression.pattern.{LynxNodePattern, LynxRelationshipPattern}
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{NodePattern, Range, RelationshipPattern}

/**
  *@description:
  */
object ConvertPatternToLynxPattern {
  def convertNodePattern(pattern: NodePattern, columnOffset: Int): LynxNodePattern = {
    LynxNodePattern(
      LynxVariable(pattern.variable.get.name, columnOffset),
      pattern.labels.map(label => LynxNodeLabel(label.name)),
      pattern.properties.map(expr => ConvertExpressionToLynxExpression.convert(expr))
    )
  }
  def convertRelationshipPattern(
      pattern: RelationshipPattern,
      columnOffset: Int
    ): LynxRelationshipPattern = {
    val relTypes = pattern.types.map(name => LynxRelationshipType(name.name))
    val relVariableName = pattern.variable.get.name
    val properties = pattern.properties.map(expr => ConvertExpressionToLynxExpression.convert(expr))
    val (lowerHop, upperHop) = pattern.length match {
      case None       => (1, 1)
      case Some(None) => (1, Int.MaxValue)
      case Some(Some(Range(a, b))) => {
        (a, b) match {
          case (_, None) => (a.get.value.toInt, Int.MaxValue)
          case (None, _) => (1, b.get.value.toInt)
          case _         => (a.get.value.toInt, b.get.value.toInt)
        }
      }
    }
    LynxRelationshipPattern(
      LynxVariable(relVariableName, columnOffset),
      relTypes,
      lowerHop,
      upperHop,
      properties,
      pattern.direction
    )
  }
}
