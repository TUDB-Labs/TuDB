package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalJoin, LogicalNode, LogicalPatternMatch}
import org.opencypher.v9_0.ast.{Match, Where}
import org.opencypher.v9_0.expressions.{EveryPath, NodePattern, Pattern, PatternElement, PatternPart, RelationshipChain, RelationshipPattern}

/**
  *@description:
  */
case class LogicalMatchTranslator(m: Match) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    //run match
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m
    val parts = patternParts.map(matchPatternPart(_)(plannerContext))
    val matched = parts.drop(1).foldLeft(parts.head)((a, b) => LogicalJoin(true)(a, b))
    val filtered = LogicalWhereTranslator(where).translate(Some(matched))

    in match {
      case None       => filtered
      case Some(left) => LogicalJoin(false)(left, filtered)
    }
  }

  private def matchPatternPart(
      patternPart: PatternPart
    )(implicit lpc: LogicalPlannerContext
    ): LogicalNode = {
    patternPart match {
      case EveryPath(element: PatternElement) => matchPattern(element)
    }
  }

  private def matchPattern(
      element: PatternElement
    )(implicit lpc: LogicalPlannerContext
    ): LogicalPatternMatch = {
    element match {
      //match (m:label1)
      case np: NodePattern =>
        LogicalPatternMatch(np, Seq.empty)

      //match ()-[]->()
      case rc @ RelationshipChain(
            leftNode: NodePattern,
            relationship: RelationshipPattern,
            rightNode: NodePattern
          ) =>
        LogicalPatternMatch(leftNode, Seq(relationship -> rightNode))

      //match ()-[]->()-...-[r:type]->(n:label2)
      case rc @ RelationshipChain(
            leftChain: RelationshipChain,
            relationship: RelationshipPattern,
            rightNode: NodePattern
          ) =>
        val mp = matchPattern(leftChain)
        LogicalPatternMatch(mp.headNode, mp.chain :+ (relationship -> rightNode))
    }
  }
}
