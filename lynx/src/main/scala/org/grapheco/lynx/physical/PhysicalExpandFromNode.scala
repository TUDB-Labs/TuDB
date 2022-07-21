package org.grapheco.lynx.physical

import org.grapheco.lynx.{AbstractPPTNode, DataFrame, ExecutionContext, LynxType, NodeFilter, PPTNode, PhysicalPlannerContext, RelationshipFilter}
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxPath
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTList, CTNode, CTRelationship}

/**
  *@author:John117
  *@createDate:2022/7/20
  *@description: refer to PPTRelationshipScan
  */
case class PhysicalExpandFromNode(
    leftNodeName: String,
    rel: RelationshipPattern,
    rightNode: NodePattern,
    direction: SemanticDirection
  )(implicit in: PPTNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPPTNode {

  override val children: Seq[PPTNode] = Seq(in)

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

  override def withChildren(children0: Seq[PPTNode]): PhysicalExpandFromNode =
    PhysicalExpandFromNode(leftNodeName, rel, rightNode, direction)(children0.head, plannerContext)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val RelationshipPattern(
      variable: Option[LogicalVariable],
      types: Seq[RelTypeName],
      length: Option[Option[Range]],
      properties: Option[Expression],
      direction2: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]
    ) = rel
    val NodePattern(
      var2,
      labels2: Seq[LabelName],
      properties2: Option[Expression],
      baseNode2: Option[LogicalVariable]
    ) = rightNode

    val schema0 = if (length.isEmpty) {
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

    val (lowerLimit, upperLimit) = length match {
      case None                    => (1, 1)
      case Some(None)              => (1, Int.MaxValue)
      case Some(Some(Range(a, b))) => (a.map(_.value.toInt).get, b.map(_.value.toInt).get)
    }

    implicit val ec = ctx.expressionContext

    DataFrame(
      df.schema ++ schema0,
      () => {
        df.records.flatMap { record0 =>
          val recordMap = df.schema.map(f => f._1).zip(record0).toMap
          val fromNode = recordMap.getOrElse(
            leftNodeName,
            throw new TuDBException(
              TuDBError.LYNX_WRONG_ARGUMENT,
              s"no argument named $leftNodeName"
            )
          )

          graphModel
            .nodeLengthExpand(
              fromNode.asInstanceOf[LynxNode],
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
            .map(triple => {
              if (length.isEmpty)
                record0 ++ Seq(triple.head.storedRelation, triple.head.endNode)
              else
                record0 ++ Seq(LynxPath(triple), triple.last.endNode)
            })
        }
      }
    )
  }

}
