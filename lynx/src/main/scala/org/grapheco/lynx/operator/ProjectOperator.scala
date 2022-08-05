package org.grapheco.lynx.operator

import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.symbols.CypherType

/**
  *@author:John117
  *@createDate:2022/8/3
  *@description: Project Operator is used to choose the properties from node or relationship
  *                or select columns if there aren't any project expressions.
  */
case class ProjectOperator(
    in: ExecutionOperator,
    columnsToSelect: Seq[(String, Option[String])],
    projectColumnExpr: Seq[(String, Expression)],
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  override val children: Seq[ExecutionOperator] = Seq(in)

  var projectSchema: Seq[(String, LynxType)] = Seq.empty
  var inColumnNames: Seq[String] = Seq.empty

  var inSchemaWithIndex: Map[String, (CypherType, Int)] = Map.empty
  var outPutSchema: Seq[(String, CypherType)] = Seq.empty

  override def openImpl(): Unit = {
    in.open()
    inColumnNames = in.outputSchema().map(os => os._1)
    if (projectColumnExpr.nonEmpty) {
      projectSchema = projectColumnExpr.map {
        case (name, expression) =>
          name -> expressionEvaluator.typeOf(expression, in.outputSchema().toMap)
      }
      inSchemaWithIndex = projectSchema.zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2)).toMap
    } else
      inSchemaWithIndex = in.outputSchema().zipWithIndex.map(x => x._1._1 -> (x._1._2, x._2)).toMap

    outPutSchema = columnsToSelect.map(column =>
      column._2.getOrElse(column._1) -> inSchemaWithIndex(column._1)._1
    )
  }

  override def getNextImpl(): RowBatch = {
    val sourceData = in.getNext()
    val resultData = if (projectSchema.nonEmpty) {
      sourceData.batchData.map(rowData => {
        val recordCtx = exprContext.withVars(inColumnNames.zip(rowData).toMap)
        val projectData = projectColumnExpr.map(col => evalExpr(col._2)(recordCtx))
        columnsToSelect.map(column => {
          projectData.apply(inSchemaWithIndex(column._1)._2)
        })
      })
    } else {
      sourceData.batchData.map(rowData => {
        columnsToSelect.map(column => {
          rowData.apply(inSchemaWithIndex(column._1)._2)
        })
      })
    }

    RowBatch(resultData)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = outPutSchema

}
