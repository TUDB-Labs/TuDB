package org.grapheco.lynx.physical

import org.grapheco.lynx.planner.{ExecutionContext, PhysicalPlannerContext}
import org.grapheco.lynx.{DataFrame, LynxType, NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxPath
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTRelationship, ListType}

/**
  *@description: move from physical
  */
case class PhysicalExpandPath(
    rel: RelationshipPattern,
    rightNode: NodePattern
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalExpandPath =
    PhysicalExpandPath(rel, rightNode)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val RelationshipPattern(
      variable: Option[LogicalVariable],
      types: Seq[RelTypeName],
      length: Option[Option[Range]],
      properties: Option[Expression],
      direction: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]
    ) = rel
    val NodePattern(
      var2,
      labels2: Seq[LabelName],
      properties2: Option[Expression],
      baseNode2: Option[LogicalVariable]
    ) = rightNode
    val schema0 = Seq(
      variable.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
      var2.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
    )
    in.schema ++ schema0
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val RelationshipPattern(
      variable: Option[LogicalVariable],
      types: Seq[RelTypeName],
      length: Option[Option[Range]],
      properties: Option[Expression],
      direction: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]
    ) = rel
    val NodePattern(
      var2,
      labels2: Seq[LabelName],
      properties2: Option[Expression],
      baseNode2: Option[LogicalVariable]
    ) = rightNode

    val schema0 = {
      if (length.isEmpty) {
        Seq(
          variable.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
          var2.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      } else {
        Seq(
          variable.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(
            CTRelationship
          ),
          var2.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      }
    }

    implicit val ec = ctx.expressionContext

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
      df.schema ++ schema0,
      () => {
        df.records.flatMap { record0 =>
          val data = graphModel
            .expand(
              record0.last.asInstanceOf[LynxNode],
              RelationshipFilter(
                types.map(_.name).map(LynxRelationshipType),
                properties
                  .map(
                    eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))
                  )
                  .getOrElse(Map.empty)
              ),
              NodeFilter(
                labels2.map(_.name).map(LynxNodeLabel),
                properties2
                  .map(
                    eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2))
                  )
                  .getOrElse(Map.empty)
              ),
              direction,
              lowerLimit,
              upperLimit
            )
          val relCypherType = schema0.head._2
          relCypherType match {
            // process relationship Path to support like (q)-[r1:KNOW]->(a)-[r2:TYPE]->(b)
            case r @ CTRelationship => {
              data.map(f => record0 ++ Seq(f.head.storedRelation, f.head.endNode))
            }
            // process relationship Path to support like (q)-[r1:KNOW]->(a)-[r2:TYPE*1..3]->(b)
            case rs @ ListType(CTRelationship) => {
              data.map(f => record0 ++ Seq(LynxPath(f), f.last.endNode))
            }
          }
        }
      }
    )
  }
}
