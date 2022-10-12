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
import org.grapheco.lynx.func.{LynxProcedure, LynxProcedureArgument}
import org.grapheco.lynx.types.{LynxValue, TypeSystem}
import org.grapheco.lynx.LynxType

import scala.collection.mutable

/** @ClassName DefaultProcedureRegistry
  * @Description TODO
  * @Author huchuan
  * @Date 2022/4/20
  * @Version 0.1
  */
class DefaultProcedureRegistry(types: TypeSystem, classes: Class[_]*)
  extends ProcedureRegistry
  with LazyLogging {
  val procedures = mutable.Map[String, CallableProcedure]()

  classes.foreach(registerAnnotatedClass)

  def registerAnnotatedClass(clazz: Class[_]): Unit = {
    val host = clazz.newInstance()

    clazz.getDeclaredMethods.foreach { method =>
      val annotation = method.getAnnotation(classOf[LynxProcedure])
      if (annotation != null) {
        val inputs = method.getParameters.map { parameter =>
          // LynxProcedureArgument("xx") or parameter name.
          val paraAnnotation = Option(parameter.getAnnotation(classOf[LynxProcedureArgument]))
          val name = paraAnnotation.map(_.name()).getOrElse(parameter.getName)
          name -> types.typeOf(parameter.getType)
        }
        val outputs = Seq("value" -> types.typeOf(method.getReturnType))
        register(
          annotation.name(),
          inputs,
          outputs,
          args => types.wrap(method.invoke(host, args))
        )
      }
    }
  }

  def register(name: String, procedure: CallableProcedure): Unit = {
    procedures(name.toLowerCase) = procedure
    logger.debug(s"registered procedure: ${procedure.signature(name)}")
  }

  def register(
      name: String,
      inputs0: Seq[(String, LynxType)],
      outputs0: Seq[(String, LynxType)],
      call0: (Seq[LynxValue]) => LynxValue
    ): Unit = {
    register(
      name,
      new CallableProcedure() {
        override val inputs: Seq[(String, LynxType)] = inputs0
        override val outputs: Seq[(String, LynxType)] = outputs0
        override def call(args: Seq[LynxValue]): LynxValue = LynxValue(call0(args))
      }
    )
  }

  override def getProcedure(prefix: List[String], name: String): Option[CallableProcedure] =
    procedures.get((prefix :+ name).mkString(".").toLowerCase)
}
