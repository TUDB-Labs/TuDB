package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalNode, LogicalSkip}
import org.opencypher.v9_0.ast.Skip

/**
  *@description:
  */
case class LogicalSkipTranslator(skip: Option[Skip]) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    skip match {
      case None             => in.get
      case Some(Skip(expr)) => LogicalSkip(expr)(in.get)
    }
  }
}
