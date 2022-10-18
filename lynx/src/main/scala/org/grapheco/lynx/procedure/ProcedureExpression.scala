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

package org.grapheco.lynx.procedure

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.expression.LynxExpression
import org.opencypher.v9_0.expressions.{Expression}
import org.opencypher.v9_0.expressions.functions.{Function => CypherFunction}

case class ProcedureExpression(
    procedure: CallableProcedure,
    args: Seq[Expression],
    aggregating: Boolean,
    funcName: String,
    functionType: CypherFunction,
    distinct: Boolean)
  extends LynxExpression
  with LazyLogging {

  // when convert AST plan to logical plan, this method determines convert to aggregation or project
  override def containsAggregate: Boolean = aggregating

  logger.debug(
    s"binding FunctionInvocation ${funcName} to procedure ${procedure}, containsAggregate: ${aggregating}"
  )
}
