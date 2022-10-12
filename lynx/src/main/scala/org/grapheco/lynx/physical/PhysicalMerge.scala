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

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.opencypher.v9_0.ast.MergeAction
import org.opencypher.v9_0.expressions.{Expression, LabelName, RelTypeName, SemanticDirection}

import scala.collection.mutable

/**
  *@description:
  */
trait MergeElement {
  def getSchema: String
}

case class MergeNode(varName: String, labels: Seq[LabelName], properties: Option[Expression])
  extends MergeElement {
  override def getSchema: String = varName
}

case class MergeRelationship(
    varName: String,
    types: Seq[RelTypeName],
    properties: Option[Expression],
    varNameLeftNode: String,
    varNameRightNode: String,
    direction: SemanticDirection)
  extends MergeElement {
  override def getSchema: String = varName
}

case class PhysicalMergeAction(
    actions: Seq[MergeAction]
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalMergeAction =
    PhysicalMergeAction(actions)(children0.head, plannerContext)

  override val children: Seq[PhysicalNode] = Seq(in)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    PhysicalSetClause(Seq.empty, actions)(in, plannerContext).execute(ctx)
  }
}

// if pattern not exists, create all elements in mergeOps
case class PhysicalMerge(
    mergeSchema: Seq[(String, LynxType)],
    mergeOps: Seq[MergeElement]
  )(implicit val in: Option[PhysicalNode],
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = in.toSeq

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalMerge =
    PhysicalMerge(mergeSchema, mergeOps)(children0.headOption, plannerContext)

  override val schema: Seq[(String, LynxType)] = mergeSchema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext

    children match {
      case Seq(pj @ PhysicalJoin(filterExpr, isSingleMatch, bigTableIndex)) => {
        val searchVar = pj.children.head.schema.toMap
        val res = children.map(_.execute).head.records
        if (res.nonEmpty) {
          plannerContext.pptContext ++= Map("MergeAction" -> false)
          DataFrame(pj.schema, () => res)
        } else {
          plannerContext.pptContext ++= Map("MergeAction" -> true)

          val toCreateSchema = mergeSchema.filter(f => !searchVar.contains(f._1))
          val toCreateList = mergeOps.filter(f => !searchVar.contains(f.getSchema))
          val nodesToCreate =
            toCreateList.filter(f => f.isInstanceOf[MergeNode]).map(f => f.asInstanceOf[MergeNode])
          val relsToCreate = toCreateList
            .filter(f => f.isInstanceOf[MergeRelationship])
            .map(f => f.asInstanceOf[MergeRelationship])

          val dependDf = pj.children.head.execute
          val anotherDf = pj.children.last

          val func = dependDf.records.map { record =>
            {
              val ctxMap = dependDf.schema.zip(record).map(x => x._1._1 -> x._2).toMap

              val mergedNodesAndRels = mutable.Map[String, Seq[LynxValue]]()

              val forceToCreate = {
                if (anotherDf.isInstanceOf[PhysicalExpandPath] || anotherDf
                      .isInstanceOf[PhysicalRelationshipScan]) true
                else false
              }

              //              createNode(nodesToCreate, mergedNodesAndRels, ctxMap, forceToCreate)
              //              createRelationship(relsToCreate, mergedNodesAndRels, ctxMap, forceToCreate)

              record ++ toCreateSchema.flatMap(f => mergedNodesAndRels(f._1))
            }
          }
          DataFrame(dependDf.schema ++ toCreateSchema, () => func)
        }
      }
      case _ => {
        // only merge situation
        val res = children.map(_.execute).head.records
        if (res.nonEmpty) {
          plannerContext.pptContext ++= Map("MergeAction" -> false)
          DataFrame(mergeSchema, () => res)
        } else {
          plannerContext.pptContext ++= Map("MergeAction" -> true)
          val createdNodesAndRels = mutable.Map[String, Seq[LynxValue]]()
          val nodesToCreate =
            mergeOps.filter(f => f.isInstanceOf[MergeNode]).map(f => f.asInstanceOf[MergeNode])
          val relsToCreate = mergeOps
            .filter(f => f.isInstanceOf[MergeRelationship])
            .map(f => f.asInstanceOf[MergeRelationship])

          //          createNode(nodesToCreate, createdNodesAndRels, Map.empty, true)
          //          createRelationship(relsToCreate, createdNodesAndRels, Map.empty, true)

          val res = Seq(mergeSchema.flatMap(f => createdNodesAndRels(f._1))).toIterator
          DataFrame(mergeSchema, () => res)
        }
      }
    }
  }
}
