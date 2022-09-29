package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalNode, LogicalProcedureCall}
import org.opencypher.v9_0.ast.{ProcedureResult, ProcedureResultItem, UnresolvedCall, Where}
import org.opencypher.v9_0.expressions.{Expression, Namespace, ProcedureName, Variable}

/**
  *@description:
  */
case class LogicalProcedureCallTranslator(c: UnresolvedCall) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    val UnresolvedCall(
      ns @ Namespace(parts: List[String]),
      pn @ ProcedureName(name: String),
      declaredArguments: Option[Seq[Expression]],
      declaredResult: Option[ProcedureResult]
    ) = c
    val call = LogicalProcedureCall(ns, pn, declaredArguments)

    declaredResult match {
      case Some(ProcedureResult(items: IndexedSeq[ProcedureResultItem], where: Option[Where])) =>
        PipedTranslators(
          Seq(
            LogicalSelectTranslator(items.map(item => {
              val ProcedureResultItem(output, Variable(varname)) = item
              varname -> output.map(_.name)
            })),
            LogicalWhereTranslator(where)
          )
        ).translate(Some(call))

      case None => call
    }
  }
}
