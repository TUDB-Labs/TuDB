package org.grapheco.lynx.rules

import org.grapheco.lynx.graph.GraphModel
import org.grapheco.lynx.PhysicalPlanOptimizerRule
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.physical.{PhysicalJoin, PhysicalMerge, PhysicalNode, PhysicalNodeScan, PhysicalRelationshipScan}
import org.opencypher.v9_0.expressions.{Literal, MapExpression, NodePattern, RelationshipPattern}

import scala.collection.mutable

/** rule to estimate two tables' size in PhysicalJoin
  * mark the bigger table's position
  */
object JoinTableSizeEstimateRule extends PhysicalPlanOptimizerRule {

  override def apply(plan: PhysicalNode, ppc: PhysicalPlannerContext): PhysicalNode =
    optimizeBottomUp(
      plan, {
        case pnode: PhysicalNode => {
          pnode.children match {
            case Seq(pj @ PhysicalJoin(filterExpr, isSingleMatch, bigTableIndex)) => {
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

  def estimate(table: PhysicalNode, ppc: PhysicalPlannerContext): Long = {
    table match {
      case ps @ PhysicalNodeScan(pattern) => estimateNodeRow(pattern, ppc.runnerContext.graphModel)
      case pr @ PhysicalRelationshipScan(rel, left, right) =>
        estimateRelationshipRow(rel, left, right, ppc.runnerContext.graphModel)
    }
  }

  def estimateTableSize(
      parent: PhysicalJoin,
      table1: PhysicalNode,
      table2: PhysicalNode,
      ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    val estimateTable1 = estimate(table1, ppc)
    val estimateTable2 = estimate(table2, ppc)
    if (estimateTable1 <= estimateTable2)
      PhysicalJoin(parent.filterExpr, parent.isSingleMatch, 1)(table1, table2, ppc)
    else PhysicalJoin(parent.filterExpr, parent.isSingleMatch, 0)(table1, table2, ppc)
  }

  def joinRecursion(
      parent: PhysicalJoin,
      ppc: PhysicalPlannerContext,
      isSingleMatch: Boolean
    ): PhysicalNode = {
    val t1 = parent.children.head
    val t2 = parent.children.last

    val table1 = t1 match {
      case pj @ PhysicalJoin(filterExpr, isSingleMatch, bigTableIndex) =>
        joinRecursion(pj, ppc, isSingleMatch)
      case pm @ PhysicalMerge(mergeSchema, mergeOps) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[PhysicalJoin], ppc, isSingleMatch)
        pm.withChildren(Seq(res))
      }
      case _ => t1
    }
    val table2 = t2 match {
      case pj @ PhysicalJoin(filterExpr, isSingleMatch, bigTableIndex) =>
        joinRecursion(pj, ppc, isSingleMatch)
      case pm @ PhysicalMerge(mergeSchema, mergeOps) => {
        val res = joinRecursion(pm.children.head.asInstanceOf[PhysicalJoin], ppc, isSingleMatch)
        pm.withChildren(Seq(res))
      }
      case _ => t2
    }

    if ((table1.isInstanceOf[PhysicalNodeScan] || table1.isInstanceOf[PhysicalRelationshipScan])
        && (table2.isInstanceOf[PhysicalNodeScan] || table2
          .isInstanceOf[PhysicalRelationshipScan])) {
      estimateTableSize(parent, table1, table2, ppc)
    } else
      PhysicalJoin(parent.filterExpr, parent.isSingleMatch, parent.bigTableIndex)(
        table1,
        table2,
        ppc
      )
  }
}
