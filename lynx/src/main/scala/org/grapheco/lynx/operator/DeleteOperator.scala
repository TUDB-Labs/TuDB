package org.grapheco.lynx.operator

import org.grapheco.lynx.operator.utils.OperatorUtils
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch, SyntaxErrorException}
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
    toDeleteEntityExprs: Seq[Expression],
    forceToDelete: Boolean,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  var inColumnNames: Seq[String] = Seq.empty
  var isDeleteDone: Boolean = false

  override def openImpl(): Unit = {
    in.open()
    inColumnNames = in.outputSchema().map(nameAndType => nameAndType._1)
  }

  override def getNextImpl(): RowBatch = {
    if (isDeleteDone) return RowBatch(Seq.empty)

    val toDeleteNodeIds = ArrayBuffer[LynxId]()
    val toDeleteRelIds = ArrayBuffer[LynxId]()

    val allInData = OperatorUtils.getOperatorAllOutputs(in).flatMap(rowBatch => rowBatch.batchData)
    allInData.foreach(rowData => {
      val variableValueByName = inColumnNames.zip(rowData).toMap
      toDeleteEntityExprs.foreach(expr => {
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
    })

    if (toDeleteNodeIds.nonEmpty)
      graphModel.deleteNodesSafely(toDeleteNodeIds.toIterator, forceToDelete)

    if (toDeleteRelIds.nonEmpty)
      graphModel.deleteRelations(toDeleteRelIds.toIterator)

    isDeleteDone = true
    RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = Seq.empty
}
