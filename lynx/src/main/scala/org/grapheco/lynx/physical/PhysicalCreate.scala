package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{Expression, LabelName, MapExpression, RelTypeName}

import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
trait CreateElement

case class CreateNode(varName: String, labels3: Seq[LabelName], properties3: Option[Expression])
  extends CreateElement

case class CreateRelationship(
    varName: String,
    types: Seq[RelTypeName],
    properties: Option[Expression],
    varNameFromNode: String,
    varNameToNode: String)
  extends CreateElement

case class PhysicalCreate(
    schemaLocal: Seq[(String, LynxType)],
    ops: Seq[CreateElement]
  )(implicit val in: Option[PhysicalNode],
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = in.toSeq

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalCreate =
    PhysicalCreate(schemaLocal, ops)(children0.headOption, plannerContext)

  override val schema: Seq[(String, LynxType)] =
    in.map(_.schema).getOrElse(Seq.empty) ++ schemaLocal

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    val df = in.map(_.execute(ctx)).getOrElse(createUnitDataFrame(Seq.empty))
    //DataFrame should be generated first
    DataFrame.cached(
      schema,
      df.records.map { record =>
        val ctxMap = df.schema.zip(record).map(x => x._1._1 -> x._2).toMap
        val nodesInput = ArrayBuffer[(String, NodeInput)]()
        val relsInput = ArrayBuffer[(String, RelationshipInput)]()

        ops.foreach(_ match {
          case CreateNode(
              varName: String,
              labels: Seq[LabelName],
              properties: Option[Expression]
              ) =>
            if (!ctxMap.contains(varName) && nodesInput.find(_._1 == varName).isEmpty) {
              nodesInput += varName -> NodeInput(
                labels.map(_.name).map(LynxNodeLabel),
                properties
                  .map {
                    case MapExpression(items) =>
                      items.map({
                        case (k, v) =>
                          LynxPropertyKey(k.name) -> eval(v)(ec.withVars(ctxMap))
                      })
                  }
                  .getOrElse(Seq.empty)
              )
            }

          case CreateRelationship(
              varName: String,
              types: Seq[RelTypeName],
              properties: Option[Expression],
              varNameFromNode: String,
              varNameToNode: String
              ) =>
            def nodeInputRef(varname: String): NodeInputRef = {
              ctxMap
                .get(varname)
                .map(x => StoredNodeInputRef(x.asInstanceOf[LynxNode].id))
                .getOrElse(
                  ContextualNodeInputRef(varname)
                )
            }

            relsInput += varName -> RelationshipInput(
              types.map(_.name).map(LynxRelationshipType),
              properties
                .map {
                  case MapExpression(items) =>
                    items.map({
                      case (k, v) =>
                        LynxPropertyKey(k.name) -> eval(v)(ec.withVars(ctxMap))
                    })
                }
                .getOrElse(Seq.empty[(LynxPropertyKey, LynxValue)]),
              nodeInputRef(varNameFromNode),
              nodeInputRef(varNameToNode)
            )
        })

        record ++ graphModel.createElements(
          nodesInput,
          relsInput,
          (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
            val created = nodesCreated.toMap ++ relsCreated
            schemaLocal.map(x => created(x._1))
          }
        )
      }.toSeq
    )
  }
}
