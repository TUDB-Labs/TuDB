package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalDelete, LogicalNode}
import org.opencypher.v9_0.ast.Delete

/**
  *@description:
  */
case class LogicalDeleteTranslator(delete: Delete) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
    LogicalDelete(delete)(in.get)
}
