package org.grapheco.lynx.physical.plan

import org.grapheco.lynx.{CypherRunnerContext, LynxType}

import scala.collection.mutable

/**
  *@description:
  */
object PhysicalPlannerContext {
  def apply(
      queryParameters: Map[String, Any],
      runnerContext: CypherRunnerContext
    ): PhysicalPlannerContext =
    new PhysicalPlannerContext(
      queryParameters.mapValues(runnerContext.typeSystem.wrap).mapValues(_.lynxType).toSeq,
      runnerContext
    )
}

case class PhysicalPlannerContext(
    parameterTypes: Seq[(String, LynxType)],
    runnerContext: CypherRunnerContext,
    var pptContext: mutable.Map[String, Any] = mutable.Map.empty) {}
