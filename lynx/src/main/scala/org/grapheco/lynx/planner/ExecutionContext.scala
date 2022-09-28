package org.grapheco.lynx.planner

import org.grapheco.lynx.ExpressionContext
import org.opencypher.v9_0.ast.Statement

/**
  *@description:
  */
case class ExecutionContext(
    physicalPlannerContext: PhysicalPlannerContext,
    statement: Statement,
    queryParameters: Map[String, Any]) {
  val expressionContext = ExpressionContext(
    this,
    queryParameters.map(x => x._1 -> physicalPlannerContext.runnerContext.typeSystem.wrap(x._2))
  )
}
