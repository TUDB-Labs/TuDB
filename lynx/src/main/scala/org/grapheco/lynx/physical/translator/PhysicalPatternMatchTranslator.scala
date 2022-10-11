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

package org.grapheco.lynx.physical.translator

import org.grapheco.lynx.logical.LogicalPatternMatch
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical.{PhysicalExpandPath, PhysicalNode, PhysicalNodeScan, PhysicalRelationshipScan}
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

/**
  *@description:
  */
case class PhysicalPatternMatchTranslator(
    patternMatch: LogicalPatternMatch
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends PhysicalNodeTranslator {
  private def planPatternMatch(
      pm: LogicalPatternMatch
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    val LogicalPatternMatch(headNode: NodePattern, chain: Seq[(RelationshipPattern, NodePattern)]) =
      pm
    chain.toList match {
      //match (m)
      case Nil => PhysicalNodeScan(headNode)(ppc)
      //match (m)-[r]-(n)
      case List(Tuple2(rel, rightNode)) => PhysicalRelationshipScan(rel, headNode, rightNode)(ppc)
      //match (m)-[r]-(n)-...-[p]-(z)
      case _ =>
        val (lastRelationship, lastNode) = chain.last
        val dropped = chain.dropRight(1)
        val part = planPatternMatch(LogicalPatternMatch(headNode, dropped))(ppc)
        PhysicalExpandPath(lastRelationship, lastNode)(part, plannerContext)
    }
  }

  override def translate(
      in: Option[PhysicalNode]
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    planPatternMatch(patternMatch)(ppc)
  }
}
