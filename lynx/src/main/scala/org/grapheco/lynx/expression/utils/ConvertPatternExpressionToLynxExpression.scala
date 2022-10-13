package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.LynxVariable
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

/**
  *@description:
  */
object ConvertPatternExpressionToLynxExpression {
  def convertNodePattern(nodePattern: NodePattern): NodePattern = {
    NodePattern(
      nodePattern.variable.map(logicalVariable => LynxVariable(logicalVariable.name, 0)),
      nodePattern.labels,
      nodePattern.properties.map(expr => ConvertExpressionToLynxExpression.convert(expr)),
      nodePattern.baseNode
    )(nodePattern.position)
  }
  def convertRelationshipPattern(relationship: RelationshipPattern): RelationshipPattern = {
    RelationshipPattern(
      relationship.variable.map(logicalVariable => LynxVariable(logicalVariable.name, 0)),
      relationship.types,
      relationship.length,
      relationship.properties.map(expr => ConvertExpressionToLynxExpression.convert(expr)),
      relationship.direction,
      relationship.legacyTypeSeparator,
      relationship.baseRel
    )(relationship.position)
  }
}
