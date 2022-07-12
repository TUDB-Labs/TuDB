package org.grapheco.lynx

import org.grapheco.lynx.RemoveNullProject.optimizeBottomUp
import org.grapheco.lynx.rules.ExtractJoinReferenceRule
import org.opencypher.v9_0.ast.AliasedReturnItem
import org.opencypher.v9_0.expressions.{Ands, Equals, Expression, FunctionInvocation, HasLabels, In, LabelName, Literal, LogicalVariable, MapExpression, NodePattern, Not, Ors, PatternExpression, Property, PropertyKeyName, RegexMatch, RelationshipPattern, Variable}
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait PhysicalPlanOptimizer {
  def optimize(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode
}

trait PhysicalPlanOptimizerRule {
  def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode

  def optimizeBottomUp(node: PPTNode, ops: PartialFunction[PPTNode, PPTNode]*): PPTNode = {
    val childrenOptimized =
      node.withChildren(node.children.map(child => optimizeBottomUp(child, ops: _*)))
    ops.foldLeft(childrenOptimized) { (optimized, op) =>
      op.lift(optimized).getOrElse(optimized)
    }
  }
}

class DefaultPhysicalPlanOptimizer(runnerContext: CypherRunnerContext)
  extends PhysicalPlanOptimizer {
  val rules = Seq[PhysicalPlanOptimizerRule](
    RemoveNullProject,
    PPTFilterPushDownRule,
//    JoinReferenceRule,
    ExtractJoinReferenceRule,
    JoinTableSizeEstimateRule
  )

  def optimize(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = {
    rules.foldLeft(plan)((optimized, rule) => rule.apply(optimized, ppc))
  }
}

object RemoveNullProject extends PhysicalPlanOptimizerRule {

  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(
    plan, {
      case pnode: PPTNode =>
        pnode.children match {
          case Seq(p @ PPTProject(ri)) if ri.items.forall {
                case AliasedReturnItem(expression, variable) => expression == variable
              } =>
            pnode.withChildren(pnode.children.filterNot(_ eq p) ++ p.children)

          case _ => pnode
        }
    }
  )
}

/** rule to PUSH the nodes or relationships' property and label in PPTFilter to NodePattern or RelationshipPattern
  * LEAVE other expressions or operations in PPTFilter.
  * like:
  * PPTFilter(where node.age>10 or node.age<5 and n.label='xxx')           PPTFilter(where node.age>10 or node.age<5)
  * ||                                                           ===>      ||
  * NodePattern(n)                                                         NodePattern(n: label='xxx')
  */
object PPTFilterPushDownRule extends PhysicalPlanOptimizerRule {
  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(
    plan, {
      case pnode: PPTNode => {
        pnode.children match {
          case Seq(pf @ PPTFilter(exprs)) => {
            val res = pptFilterPushDownRule(pf, pnode, ppc)
            if (res._2) pnode.withChildren(res._1)
            else pnode
          }
          case Seq(pj @ PPTJoin(filterExpr, isSingleMatch, bigTableIndex)) => {
            val newPPT = pptJoinPushDown(pj, ppc)
            pnode.withChildren(Seq(newPPT))
          }
          case _ => pnode
        }
      }
    }
  )

  def pptJoinPushDown(pj: PPTJoin, ppc: PhysicalPlannerContext): PPTNode = {
    val res = pj.children.map {
      case pf @ PPTFilter(expr) => {
        val res = pptFilterPushDownRule(pf, pj, ppc)
        if (res._2) res._1.head
        else pf
      }
      case pjj @ PPTJoin(filterExpr, isSingleMatch, bigTableIndex) => pptJoinPushDown(pjj, ppc)
      case f                                                       => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoinPushDown(
      propMap: Map[String, Option[Expression]],
      labelMap: Map[String, Seq[LabelName]],
      pj: PPTJoin,
      ppc: PhysicalPlannerContext
    ): PPTNode = {
    val res = pj.children.map {
      case pn @ PPTNodeScan(optional, pattern) =>
        PPTNodeScan(optional, getNewNodePattern(pattern, labelMap, propMap))(ppc)
      case pjj @ PPTJoin(filterExpr, isSingleMatch, bigTableIndex) =>
        pptFilterThenJoinPushDown(propMap, labelMap, pjj, ppc)
      case f => f
    }
    pj.withChildren(res)
  }

  def pptFilterThenJoin(
      parent: PPTFilter,
      pj: PPTJoin,
      ppc: PhysicalPlannerContext
    ): (Seq[PPTNode], Boolean) = {
    val (labelMap, propertyMap, notPushDown) = extractFromFilterExpression(parent.expr)

    val res = pptFilterThenJoinPushDown(propertyMap, labelMap, pj, ppc)

    notPushDown.size match {
      case 0 => (Seq(res), true)
      case 1 => (Seq(PPTFilter(notPushDown.head)(res, ppc)), true)
      // 2, WHERE a >= b AND c < d
      // 3, WHERE 6 >= a >= 3 AND c in [4, 5]
      // TODO: Ands Logic here may be incorrect, if we have OR clause?
      case 2 | 3 => {
        val expr = Ands(Set(notPushDown: _*))(InputPosition(0, 0, 0))
        (Seq(PPTFilter(expr)(res, ppc)), true)
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

  /** @param pf    the PPTFilter
    * @param pnode the parent of PPTFilter, to rewrite PPTFilter
    * @param ppc   context
    * @return a seq and a flag, flag == true means push-down works
    */
  def pptFilterPushDownRule(
      pf: PPTFilter,
      pnode: PPTNode,
      ppc: PhysicalPlannerContext
    ): (Seq[PPTNode], Boolean) = {
    pf.children match {
      case Seq(pns @ PPTNodeScan(optional, pattern)) => {
        val patternAndSet = pushExprToNodePattern(pf.expr, pattern)
        if (patternAndSet._3) {
          if (patternAndSet._2.isEmpty) (Seq(PPTNodeScan(optional, patternAndSet._1)(ppc)), true)
          else
            (
              Seq(
                PPTFilter(patternAndSet._2.head)(PPTNodeScan(optional, patternAndSet._1)(ppc), ppc)
              ),
              true
            )
        } else (null, false)
      }
      case Seq(prs @ PPTRelationshipScan(optional, rel, left, right)) => {
        val patternsAndSet = pushExprToRelationshipPattern(pf.expr, left, right)
        if (patternsAndSet._4) {
          if (patternsAndSet._3.isEmpty)
            (
              Seq(PPTRelationshipScan(optional, rel, patternsAndSet._1, patternsAndSet._2)(ppc)),
              true
            )
          else
            (
              Seq(
                PPTFilter(patternsAndSet._3.head)(
                  PPTRelationshipScan(optional, rel, patternsAndSet._1, patternsAndSet._2)(ppc),
                  ppc
                )
              ),
              true
            )
        } else (null, false)
      }
      case Seq(pep @ PPTExpandPath(optional, rel, right)) => {
        val expandAndSet = expandPathPushDown(pf.expr, right, pep, ppc)
        if (expandAndSet._2.isEmpty) (Seq(expandAndSet._1), true)
        else (Seq(PPTFilter(expandAndSet._2.head)(expandAndSet._1, ppc)), true)
      }
      case Seq(pj @ PPTJoin(filterExpr, isSingleMatch, bigTableIndex)) =>
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
      left: NodePattern,
      right: NodePattern
    ): (NodePattern, NodePattern, Set[Expression], Boolean) = {
    expression match {
      case hl @ HasLabels(expr, labels) => {
        expr match {
          case Variable(name) => {
            if (left.variable.get.name == name) {
              val newLeftPattern = getNewNodePattern(left, Map(name -> labels), Map.empty)
              (newLeftPattern, right, Set(), true)
            } else if (right.variable.get.name == name) {
              val newRightPattern = getNewNodePattern(right, Map(name -> labels), Map.empty)
              (left, newRightPattern, Set(), true)
            } else (left, right, Set(), false)
          }
          case _ => (left, right, Set(), false)
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
              (newLeftPattern, right, Set(), true)
            } else if (right.variable.get.name == name) {
              val newRightPattern = getNewNodePattern(
                right,
                Map.empty,
                Map(name -> Option(MapExpression(List((pkn, rhs)))(InputPosition(0, 0, 0))))
              )
              (left, newRightPattern, Set(), true)
            } else (left, right, Set(), false)
          }
          case _ => (left, right, Set(), false)
        }
      }
      case andExpr @ Ands(expressions) => {
        val (nodeLabels, nodeProperties, otherExpressions) = extractFromFilterExpression(andExpr)

        val leftPattern = getNewNodePattern(left, nodeLabels, nodeProperties)
        val rightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)

        if (otherExpressions.isEmpty) (leftPattern, rightPattern, Set(), true)
        else {
          if (otherExpressions.size > 1) {
            (
              leftPattern,
              rightPattern,
              Set(Ands(otherExpressions.toSet)(InputPosition(0, 0, 0))),
              true
            )
          } else {
            (leftPattern, rightPattern, Set(otherExpressions.head), true)
          }
        }
      }
      case _ => (left, right, Set(), false)
    }
  }

  def expandPathPushDown(
      expression: Expression,
      right: NodePattern,
      pep: PPTExpandPath,
      ppc: PhysicalPlannerContext
    ): (PPTNode, Set[Expression]) = {
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
      pptNode: PPTNode,
      ppc: PhysicalPlannerContext
    ): PPTNode = {
    pptNode match {
      case e @ PPTExpandPath(optional, rel, right) =>
        val newPEP = bottomUpExpandPath(
          nodeLabels: Map[String, Seq[LabelName]],
          nodeProperties: Map[String, Option[Expression]],
          e.children.head,
          ppc
        )
        val expandRightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        PPTExpandPath(optional, rel, expandRightPattern)(newPEP, ppc)

      case r @ PPTRelationshipScan(optional, rel, left, right) => {
        val leftPattern = getNewNodePattern(left, nodeLabels, nodeProperties)
        val rightPattern = getNewNodePattern(right, nodeLabels, nodeProperties)
        PPTRelationshipScan(optional, rel, leftPattern, rightPattern)(ppc)
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

/** rule to estimate two tables' size in PPTJoin
  * mark the bigger table's position
  */
object JoinTableSizeEstimateRule extends PhysicalPlanOptimizerRule {

  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = optimizeBottomUp(
    plan, {
      case pnode: PPTNode => {
        pnode.children match {
          case Seq(pj @ PPTJoin(filterExpr, isSingleMatch, bigTableIndex)) => {
            val res = joinRecursion(pj, ppc, isSingleMatch)
            pnode.withChildren(Seq(res))
          }
          case _ => pnode
        }
      }
    }
  )

  def estimateNodeRow(pattern: NodePattern, graphModel: GraphModel): Long = {
    val countMap = mutable.Map[String, Long]()
    val labels = pattern.labels.map(l => l.name)
    val prop = pattern.properties.map({
      case MapExpression(items) => {
        items.map(p => {
          p._2 match {
            case b: Literal => (p._1.name, b.value)
            case _          => (p._1.name, null)
          }
        })
      }
    })

    if (labels.nonEmpty) {
      val minLabelAndCount =
        labels.map(label => (label, graphModel._helper.estimateNodeLabel(label))).minBy(f => f._2)

      if (prop.isDefined) {
        prop.get
          .map(f => graphModel._helper.estimateNodeProperty(minLabelAndCount._1, f._1, f._2))
          .min
      } else minLabelAndCount._2
    } else graphModel.statistics.numNode
  }

  def estimateRelationshipRow(
      rel: RelationshipPattern,
      left: NodePattern,
      right: NodePattern,
      graphModel: GraphModel
    ): Long = {
    if (rel.types.isEmpty) graphModel.statistics.numRelationship
    else graphModel._helper.estimateRelationship(rel.types.head.name)
  }

  def estimate(table: PPTNode, ppc: PhysicalPlannerContext): Long = {
    table match {
      case ps @ PPTNodeScan(optional, pattern) =>
        estimateNodeRow(pattern, ppc.runnerContext.graphModel)
      case pr @ PPTRelationshipScan(optional, rel, left, right) =>
        estimateRelationshipRow(rel, left, right, ppc.runnerContext.graphModel)
    }
  }

  def estimateTableSize(
      parent: PPTJoin,
      table1: PPTNode,
      table2: PPTNode,
      ppc: PhysicalPlannerContext
    ): PPTNode = {
    val estimateTable1 = estimate(table1, ppc)
    val estimateTable2 = estimate(table2, ppc)
    if (estimateTable1 <= estimateTable2)
      PPTJoin(parent.filterExpr, parent.isSingleMatch, 1)(table1, table2, ppc)
    else PPTJoin(parent.filterExpr, parent.isSingleMatch, 0)(table1, table2, ppc)
  }

  def joinRecursion(
      parent: PPTJoin,
      ppc: PhysicalPlannerContext,
      isSingleMatch: Boolean
    ): PPTNode = {
    val t1 = parent.children.head
    val t2 = parent.children.last

    val table1 = t1 match {
      case pj @ PPTJoin(filterExpr, isSingleMatch, bigTableIndex) =>
        joinRecursion(pj, ppc, isSingleMatch)
      case pm @ PPTMerge(mergeSchema, mergeOps) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[PPTJoin], ppc, isSingleMatch)
        pm.withChildren(Seq(res))
      }
      case _ => t1
    }
    val table2 = t2 match {
      case pj @ PPTJoin(filterExpr, isSingleMatch, bigTableIndex) =>
        joinRecursion(pj, ppc, isSingleMatch)
      case pm @ PPTMerge(mergeSchema, mergeOps) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[PPTJoin], ppc, isSingleMatch)
        pm.withChildren(Seq(res))
      }
      case _ => t2
    }

    if ((table1.isInstanceOf[PPTNodeScan] || table1.isInstanceOf[PPTRelationshipScan])
        && (table2.isInstanceOf[PPTNodeScan] || table2.isInstanceOf[PPTRelationshipScan])) {
      estimateTableSize(parent, table1, table2, ppc)
    } else
      PPTJoin(parent.filterExpr, parent.isSingleMatch, parent.bigTableIndex)(table1, table2, ppc)
  }
}
