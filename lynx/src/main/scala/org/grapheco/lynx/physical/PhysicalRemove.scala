package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.opencypher.v9_0.ast.{RemoveItem, RemoveLabelItem, RemovePropertyItem}

/**
  *@description:
  */
case class PhysicalRemove(
    removeItems: Seq[RemoveItem]
  )(implicit val in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {

  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalRemove =
    PhysicalRemove(removeItems)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val res = df.records.map(n => {
      n.size match {
        case 1 => {
          var tmpNode: LynxNode = n.head.asInstanceOf[LynxNode]
          removeItems.foreach {
            case rp @ RemovePropertyItem(property) =>
              tmpNode = graphModel
                .removeNodesProperties(Iterator(tmpNode.id), Array(rp.property.propertyKey.name))
                .next()
                .get

            case rl @ RemoveLabelItem(variable, labels) =>
              tmpNode = graphModel
                .removeNodesLabels(Iterator(tmpNode.id), rl.labels.map(f => f.name).toArray)
                .next()
                .get
          }
          Seq(tmpNode)
        }
        case 3 => {
          var triple: Seq[LynxValue] = n
          removeItems.foreach {
            case rp @ RemovePropertyItem(property) => {
              val newRel = graphModel
                .removeRelationshipsProperties(
                  Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                  Array(property.propertyKey.name)
                )
                .next()
                .get
              triple = Seq(triple.head, newRel, triple.last)
            }

            case rl @ RemoveLabelItem(variable, labels) => {
              // TODO: An relation is able to have multi-type ???
              val newRel = graphModel
                .removeRelationshipType(
                  Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                  labels.map(f => f.name).toArray.head
                )
                .next()
                .get
              triple = Seq(triple.head, newRel, triple.last)
            }
          }
          triple
        }
      }
    })
    DataFrame.cached(schema, res.toSeq)
  }
}
