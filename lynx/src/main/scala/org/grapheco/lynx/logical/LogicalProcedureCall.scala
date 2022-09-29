package org.grapheco.lynx.logical

import org.opencypher.v9_0.expressions.{Expression, Namespace, ProcedureName}

/**
  *@description:
  */
case class LogicalProcedureCall(
    procedureNamespace: Namespace,
    procedureName: ProcedureName,
    declaredArguments: Option[Seq[Expression]])
  extends LogicalNode
