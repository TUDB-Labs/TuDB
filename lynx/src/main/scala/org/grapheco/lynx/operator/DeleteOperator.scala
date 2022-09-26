package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch, SyntaxErrorException}
import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

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

  override def openImpl(): Unit = {
    in.open()
    inColumnNames = in.outputSchema().map(nameAndType => nameAndType._1)
  }

  override def getNextImpl(): RowBatch = {
    var batchData: Seq[Seq[LynxValue]] = Seq.empty
    do {
      batchData = in.getNext().batchData
      if (batchData.isEmpty) return RowBatch(Seq.empty)

      batchData.foreach(rowData => {
        val variableValueByName = inColumnNames.zip(rowData).toMap
        toDeleteEntityExprs.foreach(expr => {
          val toDeleteEntity =
            expressionEvaluator.eval(expr)(expressionContext.withVars(variableValueByName))
          toDeleteEntity.lynxType match {
            case CTNode => {
              val id = toDeleteEntity.asInstanceOf[LynxNode].id
              graphModel.deleteNodesSafely(Iterator(id), forceToDelete)
            }
            case CTRelationship => {
              val id = toDeleteEntity.asInstanceOf[LynxRelationship].id
              graphModel.deleteRelations(Iterator(id))
            }
            case elementType =>
              throw SyntaxErrorException(s"expected Node or Relationship, but a ${elementType}")
          }
        })
      })
    } while (batchData.nonEmpty)

    RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = Seq.empty
}
