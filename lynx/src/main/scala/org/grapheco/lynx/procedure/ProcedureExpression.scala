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
import org.grapheco.lynx.{QueryRunnerContext, ProcedureUnregisteredException}
import org.opencypher.v9_0.expressions.{Expression, FunctionInvocation}
import org.opencypher.v9_0.util.InputPosition

/** @ClassName ProcedureExpression
  * @Description TODO
  * @Author huchuan
  * @Date 2022/4/20
  * @Version 0.1
  */
case class ProcedureExpression(
    val funcInov: FunctionInvocation
  )(implicit runnerContext: QueryRunnerContext)
  extends Expression
  with LazyLogging {
  val procedure: CallableProcedure = runnerContext.procedureRegistry
    .getProcedure(funcInov.namespace.parts, funcInov.functionName.name)
    .getOrElse(throw ProcedureUnregisteredException(funcInov.name))
  val args: Seq[Expression] = funcInov.args
  val aggregating: Boolean = funcInov.containsAggregate

  logger.debug(
    s"binding FunctionInvocation ${funcInov.name} to procedure ${procedure}, containsAggregate: ${aggregating}"
  )

  override def position: InputPosition = funcInov.position

  override def productElement(n: Int): Any = funcInov.productElement(n)

  override def productArity: Int = funcInov.productArity

  override def canEqual(that: Any): Boolean = funcInov.canEqual(that)

  override def containsAggregate: Boolean = funcInov.containsAggregate

  override def findAggregate: Option[Expression] = funcInov.findAggregate

}
