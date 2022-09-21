package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.lynx.{ContextualNodeInputRef, CreateElement, CreateNode, CreateRelationship, ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, NodeInput, NodeInputRef, RelationshipInput, RowBatch, StoredNodeInputRef}
import org.opencypher.v9_0.expressions.{Expression, LabelName, MapExpression, RelTypeName}

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to create nodes or relationships
  */
case class CreateOperator(
    in: Option[ExecutionOperator],
    toCreateSchema: Seq[(String, LynxType)],
    toCreateElements: Seq[CreateElement],
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = in.map(Seq(_)).getOrElse(Seq.empty)

  val nodesInput = ArrayBuffer[(String, NodeInput)]()
  val relationshipsInput = ArrayBuffer[(String, RelationshipInput)]()

  val isInDefined: Boolean = in.isDefined
  var isInDone: Boolean = false
  var createdData: Array[LynxValue] = Array.empty
  var createdSchemaWithValue: Map[String, LynxValue] = Map.empty

  var isCreateDone: Boolean = false

  var schema: Seq[(String, LynxType)] = Seq.empty

  override def openImpl(): Unit = {
    if (isInDefined) {
      in.get.open()
      schema = in
        .map(i => i.outputSchema())
        .getOrElse(Seq.empty) ++ toCreateSchema
    } else schema = toCreateSchema
  }

  override def getNextImpl(): RowBatch = {
    if (!isCreateDone) {
      if (isInDefined) {
        if (!isInDone) {
          createdData =
            OperatorUtils.getOperatorAllOutputs(in.get).flatMap(batch => batch.batchData).flatten
          createdSchemaWithValue =
            in.get.outputSchema().zip(createdData).map(f => f._1._1 -> f._2).toMap
          isInDone = true
        }
      }
      toCreateElements.foreach {
        case CreateNode(
            varName: String,
            labels: Seq[LabelName],
            properties: Option[Expression]
            ) =>
          if (!createdSchemaWithValue.contains(varName) && !nodesInput.exists(_._1 == varName)) {
            nodesInput += varName -> NodeInput(
              labels.map(_.name).map(LynxNodeLabel),
              properties
                .map {
                  case MapExpression(items) =>
                    items.map({
                      case (k, v) =>
                        LynxPropertyKey(k.name) -> expressionEvaluator
                          .eval(v)(expressionContext.withVars(createdSchemaWithValue))
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
          relationshipsInput += varName -> RelationshipInput(
            types.map(_.name).map(LynxRelationshipType),
            properties
              .map {
                case MapExpression(items) =>
                  items.map({
                    case (k, v) =>
                      LynxPropertyKey(k.name) -> expressionEvaluator
                        .eval(v)(expressionContext.withVars(createdSchemaWithValue))
                  })
              }
              .getOrElse(Seq.empty[(LynxPropertyKey, LynxValue)]),
            nodeInputRef(varNameFromNode, createdSchemaWithValue),
            nodeInputRef(varNameToNode, createdSchemaWithValue)
          )
      }
      val createdAll = createdData.toSeq ++ graphModel.createElements(
        nodesInput,
        relationshipsInput,
        (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
          val created = nodesCreated.toMap ++ relsCreated
          toCreateSchema.map(x => created(x._1))
        }
      )
      isCreateDone = true
      RowBatch(Seq(createdAll))
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {
    nodesInput.clear()
    relationshipsInput.clear()
  }

  override def outputSchema(): Seq[(String, LynxType)] = schema

  private def nodeInputRef(varname: String, ctxMap: Map[String, LynxValue]): NodeInputRef = {
    ctxMap
      .get(varname)
      .map(x => StoredNodeInputRef(x.asInstanceOf[LynxNode].id))
      .getOrElse(
        ContextualNodeInputRef(varname)
      )
  }
}
