package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.lynx.{ContextualNodeInputRef, CreateElement, CreateNode, CreateRelationship, ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, NodeInput, NodeInputRef, RelationshipInput, RowBatch, StoredNodeInputRef}
import org.opencypher.v9_0.expressions.{Expression, LabelName, MapExpression, RelTypeName}

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to create nodes and relationships
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

  val isInDefined: Boolean = in.isDefined
  var isInDone: Boolean = false
  var isCreateDone: Boolean = false

  var schema: Seq[(String, LynxType)] = Seq.empty

  override def openImpl(): Unit = {
    if (isInDefined) {
      in.get.open()
      schema = in.get.outputSchema() ++ toCreateSchema
    } else schema = toCreateSchema
  }

  override def getNextImpl(): RowBatch = {
    if (!isCreateDone) {
      if (in.isDefined) {
        val outputData = in
          .map(inOperator => {
            val inResult =
              OperatorUtils.getOperatorAllOutputs(inOperator).flatMap(batch => batch.batchData)

            if (inResult.isEmpty) {
              isCreateDone = true
              return RowBatch(Seq.empty)
            }

            val outputDataArray = ArrayBuffer[Seq[LynxValue]]()

            inResult.foreach(rowData => {
              val variableValueByName =
                outputSchema()
                  .zip(rowData)
                  .map(rowAndData => rowAndData._1._1 -> rowAndData._2)
                  .toMap

              val nodesInput = ArrayBuffer[(String, NodeInput)]()
              val relationshipsInput = ArrayBuffer[(String, RelationshipInput)]()

              processCreate(nodesInput, relationshipsInput, variableValueByName)

              val outRowData = rowData ++ graphModel.createElements(
                nodesInput,
                relationshipsInput,
                (
                    nodesCreated: Seq[(String, LynxNode)],
                    relsCreated: Seq[(String, LynxRelationship)]
                ) => {
                  val created = nodesCreated.toMap ++ relsCreated
                  toCreateSchema.map(x => created(x._1))
                }
              )
              outputDataArray.append(outRowData)
            })
            outputDataArray
          })
          .get
        isCreateDone = true
        RowBatch(outputData)
      } else {
        val nodesInput = ArrayBuffer[(String, NodeInput)]()
        val relationshipsInput = ArrayBuffer[(String, RelationshipInput)]()
        processCreate(nodesInput, relationshipsInput, Map.empty)
        val outputData = graphModel.createElements(
          nodesInput,
          relationshipsInput,
          (
              nodesCreated: Seq[(String, LynxNode)],
              relsCreated: Seq[(String, LynxRelationship)]
          ) => {
            val created = nodesCreated.toMap ++ relsCreated
            toCreateSchema.map(x => created(x._1))
          }
        )
        isCreateDone = true
        RowBatch(Seq(outputData))
      }
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema

  private def nodeInputRef(
      varname: String,
      variableValueByName: Map[String, LynxValue]
    ): NodeInputRef = {
    variableValueByName
      .get(varname)
      .map(x => StoredNodeInputRef(x.asInstanceOf[LynxNode].id))
      .getOrElse(
        ContextualNodeInputRef(varname)
      )
  }

  private def processCreate(
      nodesInput: ArrayBuffer[(String, NodeInput)],
      relationshipsInput: ArrayBuffer[(String, RelationshipInput)],
      variableValueByName: Map[String, LynxValue]
    ): Unit = {
    toCreateElements.foreach {
      case CreateNode(
          varName: String,
          labels: Seq[LabelName],
          properties: Option[Expression]
          ) =>
        if (!variableValueByName.contains(varName) && !nodesInput
              .exists(_._1 == varName)) {
          nodesInput += varName -> NodeInput(
            labels.map(_.name).map(LynxNodeLabel),
            properties
              .map {
                case MapExpression(items) =>
                  items.map({
                    case (k, v) =>
                      LynxPropertyKey(k.name) -> expressionEvaluator
                        .eval(v)(expressionContext.withVars(variableValueByName))
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
                      .eval(v)(expressionContext.withVars(variableValueByName))
                })
            }
            .getOrElse(Seq.empty[(LynxPropertyKey, LynxValue)]),
          nodeInputRef(varNameFromNode, variableValueByName),
          nodeInputRef(varNameToNode, variableValueByName)
        )
    }
  }
}