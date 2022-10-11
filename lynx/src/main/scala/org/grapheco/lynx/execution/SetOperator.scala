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
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
import org.opencypher.v9_0.ast.{SetExactPropertiesFromMapItem, SetIncludingPropertiesFromMapItem, SetItem, SetLabelItem, SetPropertyItem}
import org.opencypher.v9_0.expressions.{CaseExpression, Expression, MapExpression, Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

/**
  *@description: This operator is used to:
  *               1. update labels on nodes.
  *               2. update properties on nodes and relationships.
  */
case class SetOperator(
    in: ExecutionOperator,
    setItems: Seq[SetItem],
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  private var columnNames: Seq[String] = _
  private var columnIndexByName: Map[String, Int] = _

  override def openImpl(): Unit = {
    in.open()
    columnNames = in.outputSchema().map(f => f._1)
    columnIndexByName = columnNames.zipWithIndex.toMap
  }

  override def getNextImpl(): RowBatch = {
    val batchData = in.getNext().batchData
    if (batchData.isEmpty) return RowBatch(Seq.empty)

    val updatedBatchData = batchData.map(record => {
      val variableValueByName = columnNames.zip(record).toMap
      val variableNameByValue =
        variableValueByName.map(nameAndValue => (nameAndValue._2, nameAndValue._1))

      var updatedRecord = record
      setItems.foreach {
        case SetPropertyItem(property, propertyValueExpr) => {
          val Property(entityExpr, propKeyName) = property
          val entity =
            expressionEvaluator.eval(entityExpr)(expressionContext.withVars(variableValueByName))
          entity match {
            case LynxNull => {}
            case _ => {
              val prop = expressionEvaluator
                .eval(propertyValueExpr)(expressionContext.withVars(variableValueByName))
                .value
              val entityIndex = columnIndexByName(variableNameByValue(entity))
              updatedRecord = setProperty(
                entity,
                entityIndex,
                Array(propKeyName.name -> prop),
                updatedRecord,
                false
              )
            }
          }
        }
        case SetLabelItem(variable, labels) => {
          updatedRecord = setLabel(
            variable.name,
            labels.map(l => l.name).toArray,
            updatedRecord,
            variableValueByName
          )
        }
        case SetIncludingPropertiesFromMapItem(variable, expression) => {
          val entity =
            expressionEvaluator.eval(variable)(expressionContext.withVars(variableValueByName))
          entity match {
            case LynxNull => {}
            case _ => {
              val entityIndex = columnIndexByName(variableNameByValue(entity))
              val propValue =
                expressionEvaluator.eval(expression)(
                  expressionContext.withVars(variableValueByName)
                )
              propValue match {
                case map: LynxMap => {
                  val props =
                    map.value.map(nameAndValue => (nameAndValue._1, nameAndValue._2.value)).toArray
                  updatedRecord = setProperty(entity, entityIndex, props, updatedRecord, false)
                }
                case LynxNull => {}
                case unknown => {
                  throw new TuDBException(
                    TuDBError.LYNX_UNSUPPORTED_OPERATION,
                    s"Not support expr: ${unknown.toString}"
                  )
                }
              }
            }
          }
        }
        case SetExactPropertiesFromMapItem(variable, expression) => {
          val entity =
            expressionEvaluator.eval(variable)(expressionContext.withVars(variableValueByName))
          entity match {
            case LynxNull => {}
            case _ => {
              val propValue =
                expressionEvaluator.eval(expression)(
                  expressionContext.withVars(variableValueByName)
                )
              val entityIndex = columnIndexByName(variableNameByValue(entity))
              propValue match {
                case maskNode: LynxNode =>
                  updatedRecord = copyPropertiesFromNodeAndCleanExistProperties(
                    variable.name,
                    maskNode,
                    updatedRecord,
                    variableValueByName
                  )
                case map: LynxMap => {
                  val props =
                    map.value.map(nameAndValue => (nameAndValue._1, nameAndValue._2.value)).toArray
                  updatedRecord = setProperty(entity, entityIndex, props, updatedRecord, true)
                }
                case LynxNull => {}
                case unknown => {
                  throw new TuDBException(
                    TuDBError.LYNX_UNSUPPORTED_OPERATION,
                    s"Not support expr: ${unknown.toString}"
                  )
                }
              }
            }
          }
        }
        case unknown => {
          throw new TuDBException(
            TuDBError.LYNX_UNSUPPORTED_OPERATION,
            s"Not support setItem: ${unknown.toString}"
          )
        }
      }
      updatedRecord
    })
    RowBatch(updatedBatchData)
  }

  override def closeImpl(): Unit = {
    graphModel.write.commit
  }

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()

  private def setLabel(
      entityVar: String,
      labelNames: Array[String],
      updatedRecord: Seq[LynxValue],
      variableValueByName: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    val nodeId = variableValueByName(entityVar).asInstanceOf[LynxNode].id
    val updatedNode = graphModel
      .setNodesLabels(Iterator(nodeId), labelNames)
      .next()
      .get
    updatedRecord.updated(columnIndexByName(entityVar), updatedNode)
  }
  private def copyPropertiesFromNodeAndCleanExistProperties(
      srcNodeName: String,
      maskNode: LynxNode,
      updatedRecord: Seq[LynxValue],
      variableValueByName: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    var srcNode = variableValueByName(srcNodeName).asInstanceOf[LynxNode]
    val maskNodeProps =
      maskNode.keys.map(k => k.value -> maskNode.property(k).get.value)

    srcNode = graphModel
      .setNodesProperties(Iterator(srcNode.id), maskNodeProps.toArray, true)
      .next()
      .get
    updatedRecord.updated(columnIndexByName(srcNodeName), srcNode)
  }

  private def setProperty(
      entityValue: LynxValue,
      entityIndex: Int,
      props: Array[(String, Any)],
      updatedRecord: Seq[LynxValue],
      cleanExistProperties: Boolean
    ): Seq[LynxValue] = {
    val recordType = entityValue.lynxType
    recordType match {
      case CTNode => {
        val nodeId = entityValue.asInstanceOf[LynxNode].id
        val updatedNode =
          graphModel
            .setNodesProperties(Iterator(nodeId), props, cleanExistProperties)
            .next()
            .get
        updatedRecord.updated(entityIndex, updatedNode)
      }
      case CTRelationship => {
        val relId = entityValue.asInstanceOf[LynxRelationship].id
        val updatedRel = graphModel
          .setRelationshipsProperties(Iterator(relId), props)
          .next()
          .get
        updatedRecord.updated(entityIndex, updatedRel)
      }
    }
  }
}
