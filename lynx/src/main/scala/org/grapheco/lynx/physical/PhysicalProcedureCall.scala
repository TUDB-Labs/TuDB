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

package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType, UnknownProcedureException, WrongArgumentException}
import org.grapheco.lynx.types.property.LynxNull
import org.opencypher.v9_0.expressions.{Expression, Namespace, ProcedureName}

/**
  *@description:
  */
case class PhysicalProcedureCall(
    procedureNamespace: Namespace,
    procedureName: ProcedureName,
    declaredArguments: Option[Seq[Expression]]
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalProcedureCall =
    PhysicalProcedureCall(procedureNamespace, procedureName, declaredArguments)(plannerContext)

  val Namespace(parts: List[String]) = procedureNamespace
  val ProcedureName(name: String) = procedureName
  val arguments = declaredArguments.getOrElse(Seq.empty)
  val procedure = procedureRegistry
    .getProcedure(parts, name)
    .getOrElse { throw UnknownProcedureException(parts, name) }

  override val schema: Seq[(String, LynxType)] = procedure.outputs

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val args = declaredArguments match {
      case Some(args) => args.map(eval(_)(ctx.expressionContext))
      case None =>
        procedure.inputs.map(arg => ctx.expressionContext.params.getOrElse(arg._1, LynxNull))
    }
    val argsType = args.map(_.lynxType)
    if (procedure.checkArgumentsType(argsType)) {
      DataFrame(procedure.outputs, () => Iterator(Seq(procedure.call(args))))
    } else {
      throw WrongArgumentException(name, procedure.inputs.map(_._2), argsType)
    }
  }
}
