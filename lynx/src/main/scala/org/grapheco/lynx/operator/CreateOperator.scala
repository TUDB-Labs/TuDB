package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{CreateElement, ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch}

/**
  *@description: This operator is used to create nodes and relationships with in operator.
  */
case class CreateOperator(
    in: ExecutionOperator,
    toCreateSchema: Seq[(String, LynxType)],
    toCreateElements: Seq[CreateElement],
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext

  var isCreateDone: Boolean = false
  var schema: Seq[(String, LynxType)] = Seq.empty
  var relSchemaToCreate: Seq[(String, LynxType)] = Seq.empty
  override def openImpl(): Unit = {
    in.open()
    relSchemaToCreate = toCreateSchema.filter(s => !in.outputSchema().contains(s))
    schema = in.outputSchema() ++ relSchemaToCreate
  }

  override def getNextImpl(): RowBatch = {
    if (!isCreateDone) {
      val createdData =
        OperatorUtils.getOperatorAllOutputs(in).flatMap(batch => batch.batchData).flatten
      val createdSchemaWithValue =
        in.outputSchema().zip(createdData).map(f => f._1._1 -> f._2).toMap

      val (nodesInput, relsInput) = OperatorUtils.createNodesAndRelationships(
        toCreateElements,
        createdSchemaWithValue,
        expressionEvaluator,
        expressionContext
      )

      val createdAll = createdData.toSeq ++ graphModel.createElements(
        nodesInput,
        relsInput,
        (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
          val created = nodesCreated.toMap ++ relsCreated
          relSchemaToCreate.map(x => created(x._1))
        }
      )
      isCreateDone = true
      RowBatch(Seq(createdAll))
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema
}
