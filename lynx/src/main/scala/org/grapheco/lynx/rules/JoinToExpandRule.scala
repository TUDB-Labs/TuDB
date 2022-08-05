package org.grapheco.lynx.rules

import org.grapheco.lynx.physical.PhysicalExpandFromNode
import org.grapheco.lynx.{PPTJoin, PPTNode, PPTNodeScan, PPTRelationshipScan, PPTSelect, PPTUnwind, PhysicalPlanOptimizerRule, PhysicalPlannerContext}
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern, SemanticDirection}

/**
  * This rule is used to change joinNode to expandNode.
  * The rules take effect in the following scenarios:
  *  1. There is a reference variable between the two tables of the Join,
  *      and the reference variable in UNWIND or WITH(in physical plan, WITH will translate to SELECT)
  *
  *              PPTJoin()
  *                  ╟──PPTUnwind(Variable(friends),Variable(  friend@119))
  *                  ║   ╙──PPTSelect(Vector((friends,Some(friends))))
  *                  ║           ╙──PPTFilter()
  *                  ║               ╙──PPTRelationshipScan()
  *                  ╙──PPTFilter()
  *                      ╙──PPTRelationshipScan(friend@119)
  *
  *                       ||
  *                       \/
  *               PPTFilter()
  *                  ╙──PhysicalExpandFromNode(friend@119)
  *                      ╙──PPTUnwind(Variable(friends),Variable(friend@119))
  *                          ╙──PPTSelect(Vector((friends,Some(friends))))
  *                                  ╙──PPTRelationshipScan()
  *
  *
  */
object JoinToExpandRule extends PhysicalPlanOptimizerRule {

  override def apply(plan: PPTNode, ppc: PhysicalPlannerContext): PPTNode = {
    optimizeBottomUp(
      plan, {
        case pNode: PPTNode =>
          val res = pNode.children.map {
            case pj: PPTJoin =>
              getExpandOrPPTJoin(pj, ppc)
            case node => node
          }
          // use withChildren to replace the subtree
          pNode.withChildren(res)
      }
    )
  }

  def getExpandOrPPTJoin(pptJoin: PPTJoin, context: PhysicalPlannerContext): PPTNode = {
    // left table
    val table1 = pptJoin.children.head
    // right table
    val table2 = pptJoin.children.last

    val hasReference = table1.schema.map(f => f._1).intersect(table2.schema.map(f => f._1))

    if (hasReference.size > 1) return pptJoin // not process this kind of cypher, wait to check.

    if (hasReference.nonEmpty) {
      table1 match {
        case select: PPTSelect =>
          rewriteRightTableToTheTopOfLeftTable(hasReference.head, table1, table2, context)
        case unwind: PPTUnwind =>
          rewriteRightTableToTheTopOfLeftTable(hasReference.head, table1, table2, context)
        case relationshipScan: PPTRelationshipScan =>
          rewriteRightTableToTheTopOfLeftTable(hasReference.head, table1, table2, context)
        case _ => pptJoin
      }
    } else pptJoin
  }

  def rewriteRightTableToTheTopOfLeftTable(
      varName: String,
      leftTable: PPTNode,
      rightTable: PPTNode,
      plannerContext: PhysicalPlannerContext
    ): PPTNode = {
    rightTable match {
      case n: PPTNodeScan => n
      case r: PPTRelationshipScan => {
        val PPTRelationshipScan(
          rel: RelationshipPattern,
          leftNode: NodePattern,
          rightNode: NodePattern
        ) = r
        if (leftNode.variable.get.name == varName)
          PhysicalExpandFromNode(varName, r.rel, rightNode, r.rel.direction)(
            leftTable,
            plannerContext
          )
        else if (rightNode.variable.get.name == varName) {
          r.rel.direction match {
            case SemanticDirection.OUTGOING =>
              PhysicalExpandFromNode(varName, r.rel, leftNode, SemanticDirection.INCOMING)(
                leftTable,
                plannerContext
              )
            case SemanticDirection.INCOMING =>
              PhysicalExpandFromNode(varName, r.rel, leftNode, SemanticDirection.OUTGOING)(
                leftTable,
                plannerContext
              )
            case SemanticDirection.BOTH =>
              PhysicalExpandFromNode(varName, r.rel, leftNode, SemanticDirection.BOTH)(
                leftTable,
                plannerContext
              )
          }
        } else
          throw new TuDBException(
            TuDBError.UNKNOWN_ERROR,
            "Not support this kind of Cypher, Please report this issue."
          )
      }
      case _ =>
        rightTable.withChildren(
          rightTable.children.map(rightSubTables =>
            rewriteRightTableToTheTopOfLeftTable(varName, leftTable, rightSubTables, plannerContext)
          )
        )
    }
  }
}
