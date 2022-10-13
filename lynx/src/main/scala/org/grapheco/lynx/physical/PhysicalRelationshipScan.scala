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

package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.filters.{NodeFilter, RelationshipFilter}
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxPath
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTRelationship, ListType}

/**
  *@description:
  */
case class PhysicalRelationshipScan(
    rel: RelationshipPattern,
    leftNode: NodePattern,
    rightNode: NodePattern
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalRelationshipScan =
    PhysicalRelationshipScan(rel, leftNode, rightNode)(plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val RelationshipPattern(
      var2: Option[LogicalVariable],
      types: Seq[RelTypeName],
      length: Option[Option[Range]],
      props2: Option[Expression],
      direction: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]
    ) = rel
    val NodePattern(
      var1,
      labels1: Seq[LabelName],
      props1: Option[Expression],
      baseNode1: Option[LogicalVariable]
    ) = leftNode
    val NodePattern(
      var3,
      labels3: Seq[LabelName],
      props3: Option[Expression],
      baseNode3: Option[LogicalVariable]
    ) = rightNode

    if (length.isEmpty) {
      Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
      )
    } else {
      Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(
          CTRelationship
        ),
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
      )
    }
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val RelationshipPattern(
      var2: Option[LogicalVariable],
      types: Seq[RelTypeName],
      length: Option[Option[Range]],
      props2: Option[Expression],
      direction: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]
    ) = rel
    val NodePattern(
      var1,
      labels1: Seq[LabelName],
      props1: Option[Expression],
      baseNode1: Option[LogicalVariable]
    ) = leftNode
    val NodePattern(
      var3,
      labels3: Seq[LabelName],
      props3: Option[Expression],
      baseNode3: Option[LogicalVariable]
    ) = rightNode

    implicit val ec = ctx.expressionContext

    val schema = {
      if (length.isEmpty) {
        Seq(
          var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
          var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
          var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      } else {
        Seq(
          var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
          var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(
            CTRelationship
          ),
          var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      }
    }

    //    length:
    //      [r:XXX] = None
    //      [r:XXX*] = Some(None) // degree 1 to MAX
    //      [r:XXX*..] =Some(Some(Range(None, None))) // degree 1 to MAX
    //      [r:XXX*..3] = Some(Some(Range(None, 3)))
    //      [r:XXX*1..] = Some(Some(Range(1, None)))
    //      [r:XXX*1..3] = Some(Some(Range(1, 3)))
    val (lowerLimit, upperLimit) = length match {
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

    DataFrame(
      schema,
      () => {
        val data = graphModel
          .paths(
            NodeFilter(
              labels1.map(_.name).map(LynxNodeLabel),
              props1
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            ),
            RelationshipFilter(
              types.map(_.name).map(LynxRelationshipType),
              props2
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            ),
            NodeFilter(
              labels3.map(_.name).map(LynxNodeLabel),
              props3
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            ),
            direction,
            Option(upperLimit),
            Option(lowerLimit)
          )
        val relCypherType = schema(1)._2
        relCypherType match {
          case r @ CTRelationship => {
            data.map(f => Seq(f.head.startNode, f.head.storedRelation, f.head.endNode))
          }
          // process relationship Path to support like (a)-[r:TYPE*1..3]->(b)
          case rs @ ListType(CTRelationship) => {
            data.map(f => Seq(f.head.startNode, LynxPath(f), f.last.endNode))
          }
        }
      }
    )
  }
}
