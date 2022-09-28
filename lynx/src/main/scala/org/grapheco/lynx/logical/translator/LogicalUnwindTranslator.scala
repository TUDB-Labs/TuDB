package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.{LogicalNode, LogicalUnwind}
import org.grapheco.lynx.planner.LogicalPlannerContext
import org.opencypher.v9_0.ast.Unwind

/**
  *@description:
  */
case class LogicalUnwindTranslator(u: Unwind) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
    //    in match {
    //      case None => LogicalUnwind(u)(None)
    //      case Some(left) => LogicalJoin(isSingleMatch = false)(left, LogicalUnwind(u)(in))
    //    }
    LogicalUnwind(u)(in)
}
