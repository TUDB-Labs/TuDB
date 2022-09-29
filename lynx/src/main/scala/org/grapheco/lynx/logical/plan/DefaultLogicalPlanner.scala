package org.grapheco.lynx.logical.plan

import org.grapheco.lynx.logical.translator.LogicalQueryPartTranslator
import org.grapheco.lynx.logical.{LogicalCreateIndex, LogicalNode}
import org.grapheco.lynx.{CypherRunnerContext, UnknownASTNodeException}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{LabelName, Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.ASTNode

/**
  *@description:
  */
class DefaultLogicalPlanner(runnerContext: CypherRunnerContext) extends LogicalPlanner {
  private def translate(node: ASTNode)(implicit lpc: LogicalPlannerContext): LogicalNode = {
    node match {
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        LogicalQueryPartTranslator(part).translate(None)

      case CreateUniquePropertyConstraint(
          Variable(v1),
          LabelName(l),
          List(Property(Variable(v2), PropertyKeyName(p)))
          ) =>
        throw UnknownASTNodeException(node)

      case CreateIndex(labelName, properties) =>
        LogicalCreateIndex(labelName, properties)

      case _ =>
        throw UnknownASTNodeException(node)
    }
  }

  override def plan(statement: Statement, plannerContext: LogicalPlannerContext): LogicalNode = {
    translate(statement)(plannerContext)
  }
}
