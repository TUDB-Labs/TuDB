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

import org.grapheco.lynx.physical.{MergeElement, MergeNode, MergeRelationship, PhysicalMerge, PhysicalNode}
import org.grapheco.lynx.LynxType
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.opencypher.v9_0.ast.Merge
import org.opencypher.v9_0.expressions.{EveryPath, Expression, LabelName, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipChain, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

import scala.collection.mutable

/**
  *@description:
  */
case class PhysicalMergeTranslator(m: Merge) extends PhysicalNodeTranslator {
  def translate(
      in: Option[PhysicalNode]
    )(implicit plannerContext: PhysicalPlannerContext
    ): PhysicalNode = {
    val definedVars = in.map(_.schema.map(_._1)).getOrElse(Seq.empty).toSet
    val mergeOps = mutable.ArrayBuffer[MergeElement]()
    val mergeSchema = mutable.ArrayBuffer[(String, LynxType)]()

    m.pattern.patternParts.foreach {
      case EveryPath(element) => {
        element match {
          case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) => {
            val leftNodeName = var1.map(_.name).getOrElse(s"__NODE_${element.hashCode}")
            mergeSchema.append((leftNodeName, CTNode))
            mergeOps.append(MergeNode(leftNodeName, labels1, properties1))
          }
          case chain: RelationshipChain => {
            buildMerge(chain, definedVars, mergeSchema, mergeOps)
          }
        }
      }
    }

    PhysicalMerge(mergeSchema, mergeOps)(in, plannerContext)
  }

  private def buildMerge(
      chain: RelationshipChain,
      definedVars: Set[String],
      mergeSchema: mutable.ArrayBuffer[(String, LynxType)],
      mergeOps: mutable.ArrayBuffer[MergeElement]
    ): String = {
    val RelationshipChain(
      left,
      rp @ RelationshipPattern(
        var2: Option[LogicalVariable],
        types: Seq[RelTypeName],
        length: Option[Option[Range]],
        properties2: Option[Expression],
        direction: SemanticDirection,
        legacyTypeSeparator: Boolean,
        baseRel: Option[LogicalVariable]
      ),
      rnp @ NodePattern(var3, labels3: Seq[LabelName], properties3: Option[Expression], _)
    ) = chain

    val varRelation = var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rp.hashCode}")
    val varRightNode = var3.map(_.name).getOrElse(s"__NODE_${rnp.hashCode}")
    left match {
      //create (m)-[r]-(n), left=n
      case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
        val varLeftNode = var1.map(_.name).getOrElse(s"__NODE_${left.hashCode}")
        mergeSchema.append((varLeftNode, CTNode))
        mergeSchema.append((varRelation, CTRelationship))
        mergeSchema.append((varRightNode, CTNode))

        mergeOps.append(MergeNode(varLeftNode, labels1, properties1))
        mergeOps.append(
          MergeRelationship(varRelation, types, properties2, varLeftNode, varRightNode, direction)
        )
        mergeOps.append(MergeNode(varRightNode, labels3, properties3))

        varRightNode

      // (m)-[p]-(t)-[r]-(n), leftChain=(m)-[p]-(t)
      case leftChain: RelationshipChain =>
        // (m)-[p]-(t)
        val lastNode = buildMerge(leftChain, definedVars, mergeSchema, mergeOps)
        mergeSchema.append((varRelation, CTRelationship))
        mergeSchema.append((varRightNode, CTNode))

        mergeOps.append(
          MergeRelationship(varRelation, types, properties2, lastNode, varRightNode, direction)
        )
        mergeOps.append(MergeNode(varRightNode, labels3, properties3))

        varRightNode
    }

  }
}
