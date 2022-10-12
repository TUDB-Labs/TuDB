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
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch, SyntaxErrorException}
import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to delete nodes and relationships.
  *               1. If node to be deleted has attached edges, forceToDelete must be true otherwise throw Exception.
  */
case class DeleteOperator(
    in: ExecutionOperator,
    graphModel: GraphModel,
    entitiesToDelete: Seq[Expression],
    forceToDelete: Boolean,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var inputColumnNames: Seq[String] = Seq.empty

  override def openImpl(): Unit = {
    in.open()
    inputColumnNames = in.outputSchema().map(nameAndType => nameAndType._1)
  }

  override def getNextImpl(): RowBatch = {
    var batchData: Seq[Seq[LynxValue]] = Seq.empty
    val toDeleteNodeIds = ArrayBuffer[LynxId]()
    val toDeleteRelIds = ArrayBuffer[LynxId]()
    do {
      batchData = in.getNext().batchData
      if (batchData.isEmpty) return RowBatch(Seq.empty)

      batchData.foreach(rowData => {
        val variableValueByName = inputColumnNames.zip(rowData).toMap
        entitiesToDelete.foreach(expr => {
          val toDeleteEntity =
            expressionEvaluator.eval(expr)(expressionContext.withVars(variableValueByName))
          toDeleteEntity.lynxType match {
            case CTNode => {
              val id = toDeleteEntity.asInstanceOf[LynxNode].id
              toDeleteNodeIds.append(id)
            }
            case CTRelationship => {
              val id = toDeleteEntity.asInstanceOf[LynxRelationship].id
              toDeleteRelIds.append(id)
            }
            case elementType =>
              throw SyntaxErrorException(s"expected Node or Relationship, but a ${elementType}")
          }
        })
        if (toDeleteNodeIds.nonEmpty) {
          graphModel.deleteNodesSafely(toDeleteNodeIds.toIterator, forceToDelete)
          toDeleteNodeIds.clear()
        }
        if (toDeleteRelIds.nonEmpty) {
          graphModel.deleteRelations(toDeleteRelIds.toIterator)
          toDeleteRelIds.clear()
        }
      })

    } while (batchData.nonEmpty)

    RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = Seq.empty
}
