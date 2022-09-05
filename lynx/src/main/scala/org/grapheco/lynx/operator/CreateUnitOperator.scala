package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{CreateElement, ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch}

/**
  *@description: This operator is used to create nodes and relationships with single CREATE statement.
  */
case class CreateUnitOperator(
    toCreateSchema: Seq[(String, LynxType)],
    toCreateElements: Seq[CreateElement],
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var isCreateDone: Boolean = false
  override def openImpl(): Unit = {}

  override def getNextImpl(): RowBatch = {
    if (!isCreateDone) {
      val (nodesInput, relsInput) = OperatorUtils.createNodesAndRelationships(
        toCreateElements,
        Map.empty,
        exprEvaluator,
        expressionContext
      )
      val createdData = graphModel.createElements(
        nodesInput,
        relsInput,
        (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
          val created = nodesCreated.toMap ++ relsCreated
          toCreateSchema.map(x => created(x._1))
        }
      )
      isCreateDone = true
      RowBatch(Seq(createdData))
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = toCreateSchema
}
