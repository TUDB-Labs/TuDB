package org.grapheco.lynx.operator.utils

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.lynx.{ContextualNodeInputRef, CreateElement, CreateNode, CreateRelationship, ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, NodeInput, NodeInputRef, RelationshipInput, RowBatch, StoredNodeInputRef}
import org.opencypher.v9_0.expressions.{Expression, LabelName, MapExpression, RelTypeName}

import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
object OperatorUtils {
  def getOperatorAllOutputs(operator: ExecutionOperator): Array[RowBatch] = {
    val result = ArrayBuffer[RowBatch]()
    operator.open()
    var data = operator.getNext()
    while (data.batchData.nonEmpty) {
      result.append(data)
      data = operator.getNext()
    }
    operator.close()
    result.toArray
  }

  def createNodesAndRelationships(
      ops: Seq[CreateElement],
      ctxMap: Map[String, LynxValue],
      expressionEvaluator: ExpressionEvaluator,
      expressionContext: ExpressionContext
    ): (Array[(String, NodeInput)], Array[(String, RelationshipInput)]) = {
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
                      LynxPropertyKey(k.name) -> expressionEvaluator
                        .eval(v)(expressionContext.withVars(ctxMap))
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
                    LynxPropertyKey(k.name) -> expressionEvaluator
                      .eval(v)(expressionContext.withVars(ctxMap))
                })
            }
            .getOrElse(Seq.empty[(LynxPropertyKey, LynxValue)]),
          nodeInputRef(varNameFromNode),
          nodeInputRef(varNameToNode)
        )
    })
    (nodesInput.toArray, relsInput.toArray)
  }
}
