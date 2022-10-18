// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.expression.utils.ConvertExpressionToLynxExpression
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
    val call = LogicalProcedureCall(
      ns,
      pn,
      declaredArguments.map(exprs =>
        exprs.map(expr => ConvertExpressionToLynxExpression.convert(expr))
      )
    )

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
