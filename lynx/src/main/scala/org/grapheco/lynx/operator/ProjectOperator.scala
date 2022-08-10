package org.grapheco.lynx.operator

import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.expressions.Expression

/**
  *@author:John117
  *@createDate:2022/8/3
  *@description: Project Operator is used to choose the properties from node or relationship.
  */
case class ProjectOperator(
    in: ExecutionOperator,
    projectColumnExpr: Seq[(String, Expression)],
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  override val children: Seq[ExecutionOperator] = Seq(in)

  var projectSchema: Seq[(String, LynxType)] = Seq.empty
  var inColumnNames: Seq[String] = Seq.empty

  override def openImpl(): Unit = {
    in.open()
    projectSchema = projectColumnExpr.map {
      case (name, expression) =>
        name -> expressionEvaluator.typeOf(expression, in.outputSchema().toMap)
    }
    inColumnNames = in.outputSchema().map(os => os._1)
  }

  override def getNextImpl(): RowBatch = {
    val sourceData = in.getNext()
    val projectData = sourceData.batchData.map(rowData => {
      val recordCtx = exprContext.withVars(inColumnNames.zip(rowData).toMap)
      projectColumnExpr.map(col => evalExpr(col._2)(recordCtx))
    })
    RowBatch(projectData)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = projectSchema
}
