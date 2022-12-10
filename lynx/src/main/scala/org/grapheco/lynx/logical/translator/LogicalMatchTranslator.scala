// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.expression.utils.{ConvertPatternExpressionToLynxExpression}
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
        LogicalPatternMatch(
          ConvertPatternExpressionToLynxExpression.convertNodePattern(np),
          Seq.empty
        )

      //match ()-[]->()
      case rc @ RelationshipChain(
            leftNode: NodePattern,
            relationship: RelationshipPattern,
            rightNode: NodePattern
          ) => {
        val newLeftNodePattern =
          ConvertPatternExpressionToLynxExpression.convertNodePattern(leftNode)
        val newRelationshipPattern =
          ConvertPatternExpressionToLynxExpression.convertRelationshipPattern(relationship)
        val newRightNodePattern =
          ConvertPatternExpressionToLynxExpression.convertNodePattern(rightNode)

        LogicalPatternMatch(newLeftNodePattern, Seq(newRelationshipPattern -> newRightNodePattern))
      }

      //match ()-[]->()-...-[r:type]->(n:label2)
      case rc @ RelationshipChain(
            leftChain: RelationshipChain,
            relationship: RelationshipPattern,
            rightNode: NodePattern
          ) =>
        val mp = matchPattern(leftChain)
        val headNode = ConvertPatternExpressionToLynxExpression.convertNodePattern(mp.headNode)
        val chain = mp.chain.map(patterns => {
          (
            ConvertPatternExpressionToLynxExpression.convertRelationshipPattern(patterns._1),
            ConvertPatternExpressionToLynxExpression.convertNodePattern(patterns._2)
          )
        })
        val relationshipPattern =
          ConvertPatternExpressionToLynxExpression.convertRelationshipPattern(relationship)
        val rightNodePattern =
          ConvertPatternExpressionToLynxExpression.convertNodePattern(rightNode)
        LogicalPatternMatch(headNode, chain :+ (relationshipPattern -> rightNodePattern))
    }
  }
}
