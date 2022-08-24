package org.grapheco.lynx.operator

import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch, SyntaxErrorException}
import org.opencypher.v9_0.ast.Delete
import org.opencypher.v9_0.util.symbols.{CTNode, CTPath, CTRelationship, ListType}

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to delete nodes and relationships.
  */
case class DeleteOperator(
    deleteExpr: Delete,
    in: ExecutionOperator,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  override val children: Seq[ExecutionOperator] = Seq(in)

  val projectOperators: ArrayBuffer[ProjectOperator] = ArrayBuffer.empty

  override def openImpl(): Unit = {
    deleteExpr.expressions.foreach(expr => {
      val projectOperator =
        ProjectOperator(in, Seq(("delete", expr)), expressionEvaluator, expressionContext)
      projectOperator.open()
      projectOperators.append(projectOperator)
    })
  }

  override def getNextImpl(): RowBatch = {
    projectOperators.foreach(project => {
      val (_, elementType) = project.outputSchema().head
      var batchData = project.getNext().batchData
      elementType match {
        case CTNode => {
          while (batchData.nonEmpty) {
            val ids = batchData.flatten.map(v => v.asInstanceOf[LynxNode].id).iterator
            graphModel.deleteNodesSafely(ids, deleteExpr.forced)
            batchData = project.getNext().batchData
          }
        }
        case CTRelationship => {
          while (batchData.nonEmpty) {
            val ids = batchData.flatten.map(v => v.asInstanceOf[LynxRelationship].id).iterator
            graphModel.deleteRelations(ids)
            batchData = project.getNext().batchData
          }
        }
        case ListType(CTRelationship) => ???
        case _ =>
          throw SyntaxErrorException(s"expected Node, Path or Relationship, but a ${elementType}")
      }
    })
    RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = Seq.empty
}
