package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.LynxVariable
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipChain, RelationshipPattern}

/**
  *@description:
  */
object ConvertPatternExpressionToLynxExpression {
  def convertNodePattern(nodePattern: NodePattern): NodePattern = {
    NodePattern(
      nodePattern.variable.map(logicalVariable => LynxVariable(logicalVariable.name)),
      nodePattern.labels,
      nodePattern.properties.map(expr => ConvertExpressionToLynxExpression.convert(expr)),
      nodePattern.baseNode
    )(nodePattern.position)
  }

  def convertRelationshipPattern(relationship: RelationshipPattern): RelationshipPattern = {
    RelationshipPattern(
      relationship.variable.map(logicalVariable => LynxVariable(logicalVariable.name)),
      relationship.types,
      relationship.length,
      relationship.properties.map(expr => ConvertExpressionToLynxExpression.convert(expr)),
      relationship.direction,
      relationship.legacyTypeSeparator,
      relationship.baseRel
    )(relationship.position)
  }

  def convertRelationshipChain(rChain: RelationshipChain): RelationshipChain = {
    val newElement = rChain.element match {
      case n: NodePattern           => convertNodePattern(n)
      case chain: RelationshipChain => convertRelationshipChain(chain)
    }
    val newRelationship = convertRelationshipPattern(rChain.relationship)
    val newRightNode = convertNodePattern(rChain.rightNode)
    RelationshipChain(newElement, newRelationship, newRightNode)(rChain.position)
  }
}
