package org.grapheco.lynx.rules

import org.grapheco.lynx.physical.{PhysicalFilter, PhysicalJoin, PhysicalNode, PhysicalNodeScan, PhysicalRelationshipScan}
import org.grapheco.lynx.procedure.ProcedureExpression
import org.grapheco.lynx.PhysicalPlanOptimizerRule
import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.opencypher.v9_0.expressions.{Ands, ContainerIndex, Equals, Expression, FunctionInvocation, HasLabels, In, ListLiteral, MapExpression, NodePattern, Not, Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.InputPosition

import scala.collection.mutable.ArrayBuffer

/** rule to check is there any reference property between two match clause
  * if there is any reference property, extract it to PhysicalJoin().
  * like:
  * PhysicalJoin()                                             PhysicalJoin(m.city == n.city)
  * ||                                    ====>           ||
  * match (n:person{name:'alex'})                         match (n:person{name:'alex'})
  * match (m:person where m.city=n.city})                 match (m:person)
  */
// TODO: Too much situation unknown, add match case, when needed.

object ExtractJoinReferenceRule extends PhysicalPlanOptimizerRule {

  override def apply(plan: PhysicalNode, ppc: PhysicalPlannerContext): PhysicalNode = {
    optimizeBottomUp(
      plan, {
        case pNode: PhysicalNode =>
          val res = pNode.children.map {
            case pj: PhysicalJoin =>
              getNewPhysicalJoin(pj, ppc)
            case node => node
          }
          // use withChildren to replace the subtree
          pNode.withChildren(res)
      }
    )
  }

  def getNewPhysicalJoin(pptJoin: PhysicalJoin, ppc: PhysicalPlannerContext): PhysicalNode = {
    // left table
    val table1 = pptJoin.children.head
    // right table
    val table2 = pptJoin.children.last
    // save extracted reference expression,
    val referenceExpressionArray = ArrayBuffer[Expression]()

    // Typically, the right table of join may have reference.
    val newTable2 =
      getNewTableWithoutReferenceExpression(
        table2,
        table1.schema.map(f => f._1),
        referenceExpressionArray,
        ppc
      )

    if (referenceExpressionArray.nonEmpty) {
      PhysicalJoin(referenceExpressionArray.toSeq, pptJoin.isSingleMatch, pptJoin.bigTableIndex)(
        table1,
        newTable2,
        ppc
      )
    } else pptJoin
  }

  // return (hasReferenceExpression, noReferenceExpressionArray)
  def getReferenceFromExpression(
      expr: Expression,
      referenceSchema: Seq[String],
      referenceExpressionArray: ArrayBuffer[Expression],
      noReferenceExpressionArray: ArrayBuffer[Expression]
    ): (Seq[Expression], Seq[Expression]) = {
    expr match {
      case Ands(exprs) =>
        exprs.foreach(exp => {
          getReferenceFromExpression(
            exp,
            referenceSchema,
            referenceExpressionArray,
            noReferenceExpressionArray
          )
        })
      case e @ Equals(lhs, rhs) => {
        (lhs, rhs) match {
          case (Property(expr1, pkn1), Property(expr2, pkn2)) => {
            (expr1, expr2) match {
              case (a: Variable, b: Variable) => {
                if (referenceSchema.contains(a.name) || referenceSchema.contains(b.name))
                  referenceExpressionArray.append(e)
                else noReferenceExpressionArray.append(e)
              }
            }
          }
          case (
              ProcedureExpression(FunctionInvocation(namespace, functionName, distinct, args)),
              Variable(name)
              ) => {
            // FixMe: maybe args have other type.
            val names = args.map(f => f.asInstanceOf[Variable]) ++ Seq(name)
            val res = names.intersect(referenceSchema)
            if (res.nonEmpty) referenceExpressionArray.append(e)
            else noReferenceExpressionArray.append(e)
          }
          case default => noReferenceExpressionArray.append(e)
        }
      }
      case not @ Not(rhs) => {
        rhs match {
          case i @ In(Variable(name1), Variable(name2)) => {
            if (Seq(name1, name2).intersect(referenceSchema).nonEmpty)
              referenceExpressionArray.append(not)
            else noReferenceExpressionArray.append(not)
          }
          case e @ Equals(lhs, rhs) => {
            (lhs, rhs) match {
              case (Variable(name1), Variable(name2)) => {
                if (Seq(name1, name2).intersect(referenceSchema).nonEmpty)
                  referenceExpressionArray.append(not)
                else noReferenceExpressionArray.append(not)
              }
              case (Property(Variable(name1), PropertyKeyName(name2)), Variable(name3)) => {
                if (Seq(name1, name3).intersect(referenceSchema).nonEmpty)
                  referenceExpressionArray.append(not)
                else noReferenceExpressionArray.append(not)
              }
              case _ => noReferenceExpressionArray.append(not)
            }
          }
        }
      }
      /*
      for cypher like :
        MATCH (countryX:Country {name: 'a' }),
        (countryY:Country {name: 'b' }),
        (person:Person {id: 1 })
        WITH person, countryX, countryY
        LIMIT 1
        MATCH (city:City)-[:IS_PART_OF]->(country:Country)
        WHERE country IN [countryX, countryY]
        return country
       */

      case in @ In(lhs, rhs) => {
        (lhs, rhs) match {
          case (Variable(name), ListLiteral(expressions)) => {
            // FixMe: maybe expressions have other type
            val names = expressions.map(f => f.asInstanceOf[Variable].name) ++ Seq(name)
            val res = names.intersect(referenceSchema)
            if (res.nonEmpty) referenceExpressionArray.append(in)
            else noReferenceExpressionArray.append(in)
          }
          /*
          for cypher like:
          MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
          WITH collect(tag.id) as tags
          MATCH (:Person {id: 1 })-[:KNOWS]-(friend:Person)-[:HAS_TAG]->(tag:Tag)
          WHERE tag.id in tags
          return tag
           */
          case (Property(Variable(name1), PropertyKeyName(name2)), Variable(name3)) => {
            val names = Seq(name1, name3)
            val res = names.intersect(referenceSchema)
            if (res.nonEmpty) referenceExpressionArray.append(in)
            else noReferenceExpressionArray.append(in)
          }
          case (Variable(name1), Variable(name2)) => {
            val names = Seq(name1, name2)
            val res = names.intersect(referenceSchema)
            if (res.nonEmpty) referenceExpressionArray.append(in)
            else noReferenceExpressionArray.append(in)
          }
          case default => default
        }
      }

      /**
        * for cypher like :
        *
        * MATCH (person:Person { id: 1 })-[:KNOWS*1..2]-(friend)
          WHERE  NOT person=friend
          WITH DISTINCT friend
          MATCH (friend)<-[membership:HAS_MEMBER]-(forum)
          WHERE
              membership.joinDate > '2000-01-01'
          WITH
              forum,
              collect(friend) AS friends

          OPTIONAL MATCH (fri)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
          WHERE
              fri IN friends
          return friends
        */
      case in @ In(Variable(name), Variable(expressions)) => {
        val names = Seq(expressions) ++ Seq(name)
        val res = names.intersect(referenceSchema)
        if (res.nonEmpty) referenceExpressionArray.append(in)
        else noReferenceExpressionArray.append(in)
      }
      case p => noReferenceExpressionArray.append(p)
    }
    (referenceExpressionArray, noReferenceExpressionArray)
  }

  def getReferenceFromNodePattern(
      nodePattern: NodePattern,
      referenceSchema: Seq[String],
      referenceExpressionArray: ArrayBuffer[Expression]
    ): NodePattern = {
    val properties = nodePattern.properties
    if (properties.isDefined) {
      val expr = properties.get
      expr match {
        // TODO: Maybe properties not all in MapExpressions.
        case MapExpression(items) => {
          val noReferenceItemExpressions = ArrayBuffer[(PropertyKeyName, Expression)]()
          items.foreach {
            /*
                for cypher like:
                  match (n:Person)
                  match (m:Person) where m.city = n.city
                  return m
             */
            case ke @ (PropertyKeyName(name1), Property(Variable(name2), PropertyKeyName(name3))) => {
              if (referenceSchema.contains(name2)) {
                val leftVariable = nodePattern.variable.get.name
                val newExpr = Equals(
                  Property(
                    Variable(leftVariable)(InputPosition(0, 0, 0)),
                    PropertyKeyName(name1)(InputPosition(0, 0, 0))
                  )(InputPosition(0, 0, 0)),
                  ke._2
                )(InputPosition(0, 0, 0))
                referenceExpressionArray.append(newExpr)
              } else noReferenceItemExpressions.append(ke)
            }
            /*
            for cypher like:
              unwind [1,2,3] as tagId
              match (n: Tag) where id(n) = tagId
              return n
             */
            case pv @ (PropertyKeyName(name1), Variable(name2)) => {
              val referenceName = Seq(name1, name2).intersect(referenceSchema)
              if (referenceName.nonEmpty) {
                val leftVariable = nodePattern.variable.get.name
                val referExpr = Equals(
                  Property(Variable(leftVariable)(InputPosition(0, 0, 0)), pv._1)(
                    InputPosition(0, 0, 0)
                  ),
                  Variable(name2)(InputPosition(0, 0, 0))
                )(InputPosition(0, 0, 0))
                referenceExpressionArray.append(referExpr)
              } else noReferenceItemExpressions.append(pv)
            }
            /*
            for cypher like:
              UNWIND [2,3,4] AS s
              MATCH (u:Organisation {id: s[0]})
              return u
             */
            case pc @ (PropertyKeyName(name1), ContainerIndex(Variable(name2), idx)) => {
              val referenceName = Seq(name1, name2).intersect(referenceSchema)
              if (referenceName.nonEmpty) {
                val leftVariable = nodePattern.variable.get.name
                val referExpr = Equals(
                  Property(Variable(leftVariable)(InputPosition(0, 0, 0)), pc._1)(
                    InputPosition(0, 0, 0)
                  ),
                  pc._2
                )(InputPosition(0, 0, 0))
                referenceExpressionArray.append(referExpr)
              } else noReferenceItemExpressions.append(pc)
            }
            case p => noReferenceItemExpressions.append(p)
          }

          val newNodePattern = {
            if (noReferenceItemExpressions.nonEmpty)
              NodePattern(
                nodePattern.variable,
                nodePattern.labels,
                Option(MapExpression(noReferenceItemExpressions)(InputPosition(0, 0, 0)))
              )(InputPosition(0, 0, 0))
            else NodePattern(nodePattern.variable, nodePattern.labels, None)(InputPosition(0, 0, 0))
          }
          newNodePattern
        }
        case default => nodePattern
      }
    } else nodePattern
  }

  def getNewTableWithoutReferenceExpression(
      pptNode: PhysicalNode,
      referenceSchema: Seq[String],
      referenceExpressionArray: ArrayBuffer[Expression],
      ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    val noReferenceExpressionArray = ArrayBuffer[Expression]()
    pptNode match {
      // check is there any reference in nodePattern, if have, add to referenceExpressionArray
      case ps @ PhysicalNodeScan(nodePattern) => {
        val newNodePattern = getReferenceFromNodePattern(
          nodePattern,
          referenceSchema,
          referenceExpressionArray
        )
        PhysicalNodeScan(newNodePattern)(ppc)
      }
      // check is there any reference in PhysicalFilter, if have, add to referenceExpressionArray
      case pf @ PhysicalFilter(expr) => {
        val referAndNoRefer =
          getReferenceFromExpression(
            expr,
            referenceSchema,
            referenceExpressionArray,
            noReferenceExpressionArray
          )
        referAndNoRefer._2.size match {
          // empty PhysicalFilter, then remove it
          case 0 => pf.children.head
          // new PhysicalFilter
          case 1 => PhysicalFilter(referAndNoRefer._2.head)(pf.children.head, ppc)
          // new PhysicalFilter with Ands Expression
          case _ =>
            PhysicalFilter(Ands(referAndNoRefer._2.toSet)(InputPosition(0, 0, 0)))(
              pf.children.head,
              ppc
            )
        }
      }
      // check is there any reference in leftNodePattern and rightNodePattern, if have, add to referenceExpressionArray
      case pr @ PhysicalRelationshipScan(rel, leftNodePattern, rightNodePattern) => {
        val newLeftNodePattern = getReferenceFromNodePattern(
          leftNodePattern,
          referenceSchema,
          referenceExpressionArray
        )
        val newRightNodePattern = getReferenceFromNodePattern(
          rightNodePattern,
          referenceSchema,
          referenceExpressionArray
        )
        PhysicalRelationshipScan(rel, newLeftNodePattern, newRightNodePattern)(ppc)
      }
      case default => default
    }
  }
}
