// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.lynx.execution

import org.grapheco.lynx.graph.GraphModel
import org.grapheco.lynx.execution.utils.OperatorUtils
import org.grapheco.lynx.expression.LynxMapExpression
import org.grapheco.lynx.physical.{ContextualNodeInputRef, CreateElement, CreateNode, CreateRelationship, NodeInput, NodeInputRef, RelationshipInput, StoredNodeInputRef}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
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
                case LynxMapExpression(items) =>
                  items.map({
                    case (lynxPropertyKey, expression) =>
                      lynxPropertyKey -> expressionEvaluator
                        .eval(expression)(expressionContext.withVars(variableValueByName))
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
              case LynxMapExpression(items) =>
                items.map({
                  case (lynxPropertyKey, expression) =>
                    lynxPropertyKey -> expressionEvaluator
                      .eval(expression)(expressionContext.withVars(variableValueByName))
                })
            }
            .getOrElse(Seq.empty[(LynxPropertyKey, LynxValue)]),
          nodeInputRef(varNameFromNode, variableValueByName),
          nodeInputRef(varNameToNode, variableValueByName)
        )
    }
  }
}
