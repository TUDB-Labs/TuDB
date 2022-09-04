package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch, SyntaxErrorException}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

/**
  *@description: This operator is used to delete nodes and relationships. The data from 'in' all need to be deleted.
  */
case class DeleteOperator(
    in: ExecutionOperator,
    graphModel: GraphModel,
    forceToDelete: Boolean,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  override val children: Seq[ExecutionOperator] = Seq(in)

  var deleteTypes: Seq[LynxType] = Seq.empty

  override def openImpl(): Unit = {
    in.open()
    deleteTypes = in.outputSchema().map(nameAndType => nameAndType._2).distinct
  }

  override def getNextImpl(): RowBatch = {
    var batchData: Seq[LynxValue] = Seq.empty
    do {
      batchData = in.getNext().batchData.flatten
      deleteTypes.foreach {
        case CTNode => {
          val ids = batchData
            .filter(v => v.isInstanceOf[LynxNode])
            .map(v => v.asInstanceOf[LynxNode].id)
            .iterator
          graphModel.deleteNodesSafely(ids, forceToDelete)
        }
        case CTRelationship => {
          val ids = batchData
            .filter(v => v.isInstanceOf[LynxRelationship])
            .map(v => v.asInstanceOf[LynxRelationship].id)
            .iterator
          graphModel.deleteRelations(ids)
        }
        case elementType =>
          throw SyntaxErrorException(s"expected Node or Relationship, but a ${elementType}")
      }
    } while (batchData.nonEmpty)

    RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = Seq.empty
}
