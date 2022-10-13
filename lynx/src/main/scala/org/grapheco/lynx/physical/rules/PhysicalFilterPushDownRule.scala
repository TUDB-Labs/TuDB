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

package org.grapheco.lynx.physical.rules

import org.grapheco.lynx.PhysicalPlanOptimizerRule
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical.{PhysicalExpandPath, PhysicalFilter, PhysicalJoin, PhysicalNode, PhysicalNodeScan, PhysicalRelationshipScan}
import org.opencypher.v9_0.expressions.{Ands, Equals, Expression, HasLabels, LabelName, MapExpression, NodePattern, Property, PropertyKeyName, RegexMatch, RelTypeName, RelationshipPattern, Variable}
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** rule to PUSH the nodes or relationships' property and label in PhysicalFilter to NodePattern or RelationshipPattern
  * LEAVE other expressions or operations in PhysicalFilter.
  * like:
  * PhysicalFilter(where node.age>10 or node.age<5 and n.label='xxx')           PhysicalFilter(where node.age>10 or node.age<5)
  * ||                                                           ===>      ||
  * NodePattern(n)                                                         NodePattern(n: label='xxx')
  */
object PhysicalFilterPushDownRule extends PhysicalPlanOptimizerRule {
  override def apply(plan: PhysicalNode, ppc: PhysicalPlannerContext): PhysicalNode =
    optimizeBottomUp(
      plan, {
        case pnode: PhysicalNode => {
          pnode.children match {
            case Seq(pf @ PhysicalFilter(exprs)) => {
              val res = pptFilterPushDownRule(pf, pnode, ppc)
              if (res._2) pnode.withChildren(res._1)
              else pnode
            }
            case Seq(pj @ PhysicalJoin(filterExpr, isSingleMatch, bigTableIndex)) => {
              val newPhysical = pptJoinPushDown(pj, ppc)
              pnode.withChildren(Seq(newPhysical))
            }
            case _ => pnode
          }
        }
      }
    )

  def pptJoinPushDown(pj: PhysicalJoin, ppc: PhysicalPlannerContext): PhysicalNode = {
    val res = pj.children.map {
      case pf @ PhysicalFilter(expr) => {
        val res = pptFilterPushDownRule(pf, pj, ppc)
        if (res._2) res._1.head
        else pf
      }
      case pjj @ PhysicalJoin(filterExpr, isSingleMatch, bigTableIndex) => pptJoinPushDown(pjj, ppc)
      case f                                                            => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoinPushDown(
      propMap: Map[String, Option[Expression]],
      labelMap: Map[String, Seq[LabelName]],
      pj: PhysicalJoin,
      ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    val res = pj.children.map {
      case pn @ PhysicalNodeScan(pattern) =>
        PhysicalNodeScan(getNewNodePattern(pattern, labelMap, propMap))(ppc)
      case pr @ PhysicalRelationshipScan(
            rel: RelationshipPattern,
            leftNode: NodePattern,
            rightNode: NodePattern
          ) =>
        PhysicalRelationshipScan(
          rel,
          getNewNodePattern(leftNode, labelMap, propMap),
          getNewNodePattern(rightNode, labelMap, propMap)
        )(ppc)
      case pjj @ PhysicalJoin(filterExpr, isSingleMatch, bigTableIndex) =>
        pptFilterThenJoinPushDown(propMap, labelMap, pjj, ppc)
      case f => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoin(
      parent: PhysicalFilter,
      pj: PhysicalJoin,
      ppc: PhysicalPlannerContext
    ): (Seq[PhysicalNode], Boolean) = {
    val (labelMap, propertyMap, notPushDown) = extractFromFilterExpression(parent.expr)

    val res = pptFilterThenJoinPushDown(propertyMap, labelMap, pj, ppc)

    notPushDown.size match {
      case 0 => (Seq(res), true)
      case 1 => (Seq(PhysicalFilter(notPushDown.head)(res, ppc)), true)
      // 2, WHERE a >= b AND c < d
      // 3, WHERE 6 >= a >= 3 AND c in [4, 5]
      // TODO: Ands Logic here may be incorrect, if we have OR clause?
      case 2 | 3 => {
        val expr = Ands(Set(notPushDown: _*))(InputPosition(0, 0, 0))
        (Seq(PhysicalFilter(expr)(res, ppc)), true)
      }
    }
  }

  def extractFromFilterExpression(
      expression: Expression
    ): (Map[String, Seq[LabelName]], Map[String, Option[Expression]], Seq[Expression]) = {
    val propertyMap: mutable.Map[String, Option[Expression]] = mutable.Map.empty
    val labelMap: mutable.Map[String, Seq[LabelName]] = mutable.Map.empty
    val notPushDown: ArrayBuffer[Expression] = ArrayBuffer.empty
    val propItems: mutable.Map[String, ArrayBuffer[(PropertyKeyName, Expression)]] =
      mutable.Map.empty
    val regexPattern: mutable.Map[String, ArrayBuffer[RegexMatch]] = mutable.Map.empty

    extractParamsFromFilterExpression(expression, labelMap, propItems, regexPattern, notPushDown)

    propItems.foreach {
      case (name, exprs) =>
        exprs.size match {
          case 0 => {}
          case _ => {
            propertyMap += name -> Option(MapExpression(List(exprs: _*))(InputPosition(0, 0, 0)))
            //            if (regexPattern.isEmpty) propertyMap += name -> Option(MapExpression(List(exprs: _*))(InputPosition(0, 0, 0)))
            //            else {
            //              val regexSet: Set[Expression] = regexPattern(name).toSet
            //              val mpSet: Set[Expression] = Set(MapExpression(List(exprs: _*))(InputPosition(0, 0, 0)))
            //
            //              propertyMap += name -> Option(Ands(mpSet ++ regexSet)(InputPosition(0, 0, 0)))
            //            }
          }
        }
    }

    (labelMap.toMap, propertyMap.toMap, notPushDown)
  }

  def extractParamsFromFilterExpression(
      filters: Expression,
      labelMap: mutable.Map[String, Seq[LabelName]],
      propMap: mutable.Map[String, ArrayBuffer[(PropertyKeyName, Expression)]],
      regexPattern: mutable.Map[String, ArrayBuffer[RegexMatch]],
      notPushDown: ArrayBuffer[Expression]
    ): Unit = {

    filters match {
      case e @ Equals(Property(expr, pkn), rhs) => {
        expr match {
          case Variable(name) => {
            if (propMap.contains(name)) propMap(name).append((pkn, rhs))
            else propMap += name -> ArrayBuffer((pkn, rhs))
          }
        }
      }
      case hl @ HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            labelMap += name -> labels
          }
        }
      }
      //      case rj@RegexMatch(lhs, rhs) => {
      //        lhs match {
      //          case p@Property(expr, propertyKeyName) =>{
      //            expr match {
      //              case Variable(n) =>
      //                if (regexPattern.contains(n)) regexPattern(n).append(rj)
      //                else regexPattern += n -> ArrayBuffer(rj)
      //            }
      //          }
      //        }
      //      }
      case a @ Ands(andExpress) =>
        andExpress.foreach(exp =>
          extractParamsFromFilterExpression(exp, labelMap, propMap, regexPattern, notPushDown)
        )
      case other => notPushDown += other
    }
  }

  /** @param pf    the PhysicalFilter
    * @param pnode the parent of PhysicalFilter, to rewrite PhysicalFilter
    * @param ppc   context
    * @return a seq and a flag, flag == true means push-down works
    */
  def pptFilterPushDownRule(
      pf: PhysicalFilter,
      pnode: PhysicalNode,
      ppc: PhysicalPlannerContext
    ): (Seq[PhysicalNode], Boolean) = {
    pf.children match {
      case Seq(pns @ PhysicalNodeScan(pattern)) => {
        val patternAndSet = pushExprToNodePattern(pf.expr, pattern)
        if (patternAndSet._3) {
          if (patternAndSet._2.isEmpty) (Seq(PhysicalNodeScan(patternAndSet._1)(ppc)), true)
          else
            (
              Seq(
                PhysicalFilter(patternAndSet._2.head)(PhysicalNodeScan(patternAndSet._1)(ppc), ppc)
              ),
              true
            )
        } else (null, false)
      }
      case Seq(prs @ PhysicalRelationshipScan(rel, left, right)) => {
        val patternsAndSet = pushExprToRelationshipPattern(pf.expr, rel, left, right)
        if (patternsAndSet._5) {
          if (patternsAndSet._4.isEmpty)
            (
              Seq(
                PhysicalRelationshipScan(patternsAndSet._1, patternsAndSet._2, patternsAndSet._3)(
                  ppc
                )
              ),
              true
            )
          else
            (
              Seq(
                PhysicalFilter(patternsAndSet._4.head)(
                  PhysicalRelationshipScan(patternsAndSet._1, patternsAndSet._2, patternsAndSet._3)(
                    ppc
                  ),
                  ppc
                )
              ),
              true
            )
        } else (null, false)
      }
      case Seq(pep @ PhysicalExpandPath(rel, right)) => {
        val expandAndSet = expandPathPushDown(pf.expr, right, pep, ppc)
        if (expandAndSet._2.isEmpty) (Seq(expandAndSet._1), true)
        else (Seq(PhysicalFilter(expandAndSet._2.head)(expandAndSet._1, ppc)), true)
      }
      case Seq(pj @ PhysicalJoin(filterExpr, isSingleMatch, bigTableIndex)) =>
        pptFilterThenJoin(pf, pj, ppc)
      case _ => (null, false)
    }
  }

  def pushExprToNodePattern(
      expression: Expression,
      pattern: NodePattern
    ): (NodePattern, Set[Expression], Boolean) = {
    expression match {
      case e @ Equals(Property(expr, pkn), rhs) => {
        expr match {
          case Variable(name) => {
            if (pattern.variable.get.name == name) {
              val newPattern = getNewNodePattern(
                pattern,
                Map.empty,
                Map(name -> Option(MapExpression(List((pkn, rhs)))(e.position)))
              )
              (newPattern, Set.empty, true)
            } else (pattern, Set(expression), true)
          }
        }
      }
      case hl @ HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            val newPattern = getNewNodePattern(pattern, Map(name -> labels), Map.empty)
            (newPattern, Set.empty, true)
          }
        }
      }
      case andExpr @ Ands(exprs) => handleNodeAndsExpression(andExpr, pattern)
      case _                     => (pattern, Set.empty, false)
    }
  }

  def pushExprToRelationshipPattern(
      expression: Expression,
      rel: RelationshipPattern,
      left: NodePattern,
      right: NodePattern
    ): (RelationshipPattern, NodePattern, NodePattern, Set[Expression], Boolean) = {
    expression match {
      case hl @ HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            if (left.variable.get.name == name) {
              val newLeftPattern = getNewNodePattern(left, Map(name -> labels), Map.empty)
              (rel, newLeftPattern, right, Set(), true)
            } else if (right.variable.get.name == name) {
              val newRightPattern = getNewNodePattern(right, Map(name -> labels), Map.empty)
              (rel, left, newRightPattern, Set(), true)
            } else if (rel.variable.get.name == name) {
              (
                getNewRelationshipPattern(
                  rel,
                  Map(name -> labels.map(f => RelTypeName(f.name)(f.position))),
                  Map.empty
                ),
                left,
                right,
                Set(),
                true
              )
            } else (rel, left, right, Set(), false)
          }
          case _ => (rel, left, right, Set(), false)
        }
      }
      case e @ Equals(Property(map, pkn), rhs) => {
        map match {
          case Variable(name) => {
            if (left.variable.get.name == name) {
              val newLeftPattern = getNewNodePattern(
                left,
                Map.empty,
                Map(name -> Option(MapExpression(List((pkn, rhs)))(InputPosition(0, 0, 0))))
              )
              (rel, newLeftPattern, right, Set(), true)
            } else if (right.variable.get.name == name) {
              val newRightPattern = getNewNodePattern(
                right,
                Map.empty,
                Map(name -> Option(MapExpression(List((pkn, rhs)))(InputPosition(0, 0, 0))))
              )
              (rel, left, newRightPattern, Set(), true)
            } else if (rel.variable.get.name == name) {
              (
                getNewRelationshipPattern(
                  rel,
                  Map.empty,
                  Map(name -> Option(MapExpression(List((pkn, rhs)))(InputPosition(0, 0, 0))))
                ),
                left,
                right,
                Set(),
                true
              )
            } else (rel, left, right, Set(), false)
          }
          case _ => (rel, left, right, Set(), false)
        }
      }
      case andExpr @ Ands(expressions) => {
        val (nodeLabels, nodeProperties, otherExpressions) = extractFromFilterExpression(andExpr)

        val leftPattern = getNewNodePattern(left, nodeLabels, nodeProperties)
        val rightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        val relPattern = getNewRelationshipPattern(
          rel,
          nodeLabels.map(f => f._1 -> f._2.map(t => RelTypeName(t.name)(t.position))),
          nodeProperties
        )
        if (otherExpressions.isEmpty) (relPattern, leftPattern, rightPattern, Set(), true)
        else {
          if (otherExpressions.size > 1) {
            (
              relPattern,
              leftPattern,
              rightPattern,
              Set(Ands(otherExpressions.toSet)(InputPosition(0, 0, 0))),
              true
            )
          } else {
            (relPattern, leftPattern, rightPattern, Set(otherExpressions.head), true)
          }
        }
      }
      case _ => (rel, left, right, Set(), false)
    }
  }

  def expandPathPushDown(
      expression: Expression,
      right: NodePattern,
      pep: PhysicalExpandPath,
      ppc: PhysicalPlannerContext
    ): (PhysicalNode, Set[Expression]) = {
    val (nodeLabels, nodeProperties, otherExpressions) = extractFromFilterExpression(expression)

    val topExpandPath = bottomUpExpandPath(nodeLabels, nodeProperties, pep, ppc)

    if (otherExpressions.isEmpty) (topExpandPath, Set())
    else {
      if (otherExpressions.size > 1)
        (topExpandPath, Set(Ands(otherExpressions.toSet)(InputPosition(0, 0, 0))))
      else (topExpandPath, Set(otherExpressions.head))
    }
  }

  def bottomUpExpandPath(
      nodeLabels: Map[String, Seq[LabelName]],
      nodeProperties: Map[String, Option[Expression]],
      pptNode: PhysicalNode,
      ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    pptNode match {
      case e @ PhysicalExpandPath(rel, right) =>
        val newPEP = bottomUpExpandPath(
          nodeLabels: Map[String, Seq[LabelName]],
          nodeProperties: Map[String, Option[Expression]],
          e.children.head,
          ppc
        )
        val expandRightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        PhysicalExpandPath(rel, expandRightPattern)(newPEP, ppc)

      case r @ PhysicalRelationshipScan(rel, left, right) => {
        val leftPattern = getNewNodePattern(left, nodeLabels, nodeProperties)
        val rightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        PhysicalRelationshipScan(rel, leftPattern, rightPattern)(ppc)
      }
    }
  }

  def getNewNodePattern(
      node: NodePattern,
      nodeLabels: Map[String, Seq[LabelName]],
      nodeProperties: Map[String, Option[Expression]]
    ): NodePattern = {
    val labelCheck = nodeLabels.get(node.variable.get.name)
    val label = {
      if (labelCheck.isDefined) (labelCheck.get ++ node.labels).distinct
      else node.labels
    }
    val propCheck = nodeProperties.get(node.variable.get.name)
    val props = {
      if (propCheck.isDefined) {
        if (node.properties.isDefined)
          Option(Ands(Set(node.properties.get, propCheck.get.get))(InputPosition(0, 0, 0)))
        else propCheck.get
      } else node.properties
    }
    NodePattern(node.variable, label, props, node.baseNode)(node.position)
  }
  def getNewRelationshipPattern(
      rel: RelationshipPattern,
      relTypes: Map[String, Seq[RelTypeName]],
      relProperties: Map[String, Option[Expression]]
    ): RelationshipPattern = {
    val labelCheck = relTypes.get(rel.variable.get.name)
    val label = {
      if (labelCheck.isDefined) (labelCheck.get ++ rel.types).distinct
      else rel.types
    }
    val propCheck = relProperties.get(rel.variable.get.name)
    val props = {
      if (propCheck.isDefined) {
        if (rel.properties.isDefined)
          Option(Ands(Set(rel.properties.get, propCheck.get.get))(InputPosition(0, 0, 0)))
        else propCheck.get
      } else rel.properties
    }
    RelationshipPattern(rel.variable, label, rel.length, props, rel.direction)(rel.position)
  }

  def handleNodeAndsExpression(
      andExpr: Expression,
      pattern: NodePattern
    ): (NodePattern, Set[Expression], Boolean) = {
    val (pushLabels, propertiesMap, notPushDown) = extractFromFilterExpression(andExpr)

    val newNodePattern = getNewNodePattern(pattern, pushLabels, propertiesMap)

    if (notPushDown.size > 1)
      (newNodePattern, Set(Ands(notPushDown.toSet)(InputPosition(0, 0, 0))), true)
    else (newNodePattern, notPushDown.toSet, true)
  }
}
