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
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.opencypher.v9_0.ast.{MergeAction, OnCreate, OnMatch, SetExactPropertiesFromMapItem, SetIncludingPropertiesFromMapItem, SetItem, SetLabelItem, SetPropertyItem}
import org.opencypher.v9_0.expressions.{CaseExpression, MapExpression, Property, Variable}

/**
  *@description:
  */
case class PhysicalSetClause(
    var setItems: Seq[SetItem],
    mergeAction: Seq[MergeAction] = Seq.empty
  )(implicit val in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {

  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalSetClause =
    PhysicalSetClause(setItems)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)

    setItems = {
      if (mergeAction.nonEmpty) {
        val isCreate = plannerContext.pptContext("MergeAction").asInstanceOf[Boolean]
        if (isCreate)
          mergeAction.find(p => p.isInstanceOf[OnCreate]).get.asInstanceOf[OnCreate].action.items
        else mergeAction.find(p => p.isInstanceOf[OnMatch]).get.asInstanceOf[OnMatch].action.items
      } else setItems
    }

    // TODO: batch process , not Iterator(one) !!!
    val res = df.records.map(n => {
      val ctxMap = df.schema.zip(n).map(x => x._1._1 -> x._2).toMap

      n.size match {
        // set node
        case 1 => {
          var tmpNode = n.head.asInstanceOf[LynxNode]
          setItems.foreach {
            case sp @ SetPropertyItem(property, literalExpr) => {
              val Property(map, keyName) = property
              map match {
                case v @ Variable(name) => {
                  val data = Array(
                    keyName.name -> eval(literalExpr)(ctx.expressionContext.withVars(ctxMap)).value
                  )
                  tmpNode =
                    graphModel.setNodesProperties(Iterator(tmpNode.id), data, false).next().get
                }
                case cp @ CaseExpression(expression, alternatives, default) => {
                  val res = eval(cp)(ctx.expressionContext.withVars(ctxMap))
                  res match {
                    case LynxNull => tmpNode = n.head.asInstanceOf[LynxNode]
                    case _ => {
                      val data = Array(
                        keyName.name -> eval(literalExpr)(
                          ctx.expressionContext.withVars(ctxMap)
                        ).value
                      )
                      tmpNode = graphModel
                        .setNodesProperties(Iterator(res.asInstanceOf[LynxNode].id), data, false)
                        .next()
                        .get
                    }
                  }
                }
              }
            }
            case sl @ SetLabelItem(variable, labels) => {
              tmpNode = graphModel
                .setNodesLabels(Iterator(tmpNode.id), labels.map(f => f.name).toArray)
                .next()
                .get
            }
            case si @ SetIncludingPropertiesFromMapItem(variable, expression) => {
              expression match {
                case MapExpression(items) => {
                  val data = items.map(f =>
                    f._1.name -> eval(f._2)(ctx.expressionContext.withVars(ctxMap)).value
                  )
                  tmpNode = graphModel
                    .setNodesProperties(Iterator(tmpNode.id), data.toArray, false)
                    .next()
                    .get
                }
              }
            }
            case sep @ SetExactPropertiesFromMapItem(variable, expression) => {
              expression match {
                case MapExpression(items) => {
                  val data = items.map(f =>
                    f._1.name -> eval(f._2)(ctx.expressionContext.withVars(ctxMap)).value
                  )
                  tmpNode = graphModel
                    .setNodesProperties(Iterator(tmpNode.id), data.toArray, true)
                    .next()
                    .get
                }
              }
            }
          }
          Seq(tmpNode)
        }
        // set join TODO copyNode
        case 2 => {
          var tmpNode = Seq[LynxValue]()
          setItems.foreach {
            case sep @ SetExactPropertiesFromMapItem(variable, expression) => {
              expression match {
                case v @ Variable(name) => {
                  val srcNode = ctxMap(variable.name).asInstanceOf[LynxNode]
                  val maskNode = ctxMap(name).asInstanceOf[LynxNode]
                  //                  tmpNode = graphModel.copyNode(srcNode, maskNode, ctx.tx)
                }
              }
            }
          }
          tmpNode
        }
        // set relationship
        case 3 => {
          var triple = n
          setItems.foreach {
            case sp @ SetPropertyItem(property, literalExpr) => {
              val Property(variable, keyName) = property
              val data = Array(
                keyName.name -> eval(literalExpr)(ctx.expressionContext.withVars(ctxMap)).value
              )
              val newRel = graphModel
                .setRelationshipsProperties(
                  Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                  data
                )
                .next()
                .get
              triple = Seq(triple.head, newRel, triple.last)
            }
            case sl @ SetLabelItem(variable, labels) => {
              // TODO: An relation is able to have multi-type ???
              val newRel = graphModel
                .setRelationshipsType(
                  Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                  labels.map(f => f.name).toArray.head
                )
                .next()
                .get
              triple = Seq(triple.head, newRel, triple.last)
            }
            case si @ SetIncludingPropertiesFromMapItem(variable, expression) => {
              expression match {
                case MapExpression(items) => {
                  items.foreach(f => {
                    val data =
                      Array(f._1.name -> eval(f._2)(ctx.expressionContext.withVars(ctxMap)).value)
                    val newRel = graphModel
                      .setRelationshipsProperties(
                        Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                        data
                      )
                      .next()
                      .get
                    triple = Seq(triple.head, newRel, triple.last)
                  })
                }
              }
            }
          }
          triple
        }
      }
    })

    DataFrame.cached(schema, res.toSeq)
  }
}
