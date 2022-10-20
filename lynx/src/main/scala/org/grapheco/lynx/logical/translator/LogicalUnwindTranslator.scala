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

import org.grapheco.lynx.expression.LynxVariable
import org.grapheco.lynx.expression.utils.ConvertExpressionToLynxExpression
import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalNode, LogicalUnwind}
import org.opencypher.v9_0.ast.Unwind

/**
  *@description:
  */
case class LogicalUnwindTranslator(u: Unwind) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    //    in match {
    //      case None => LogicalUnwind(u)(None)
    //      case Some(left) => LogicalJoin(isSingleMatch = false)(left, LogicalUnwind(u)(in))
    //    }

    LogicalUnwind(
      LynxVariable(u.variable.name),
      ConvertExpressionToLynxExpression.convert(u.expression)
    )(in)
  }
}
