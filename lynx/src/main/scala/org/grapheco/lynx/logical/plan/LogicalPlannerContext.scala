package org.grapheco.lynx.logical.plan

import org.grapheco.lynx.{CypherRunnerContext, LynxType}

/**
  *@description:
  */
object LogicalPlannerContext {
  def apply(
      queryParameters: Map[String, Any],
      runnerContext: CypherRunnerContext
    ): LogicalPlannerContext =
    new LogicalPlannerContext(
      queryParameters.mapValues(runnerContext.typeSystem.wrap).mapValues(_.lynxType).toSeq,
      runnerContext
    )
}

case class LogicalPlannerContext(
    parameterTypes: Seq[(String, LynxType)],
    runnerContext: CypherRunnerContext)
