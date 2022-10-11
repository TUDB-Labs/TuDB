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

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.LynxType

trait CallableProcedure {
  val inputs: Seq[(String, LynxType)]
  val outputs: Seq[(String, LynxType)]

  def call(args: Seq[LynxValue]): LynxValue

  def signature(name: String) =
    s"$name(${inputs.map(x => Seq(x._1, x._2).mkString(":")).mkString(",")})"

  def checkArgumentsNumber(actualNumber: Int): Boolean = actualNumber == inputs.size

  def checkArgumentsType(actualArgumentsType: Seq[LynxType]): Boolean = {
    actualArgumentsType.size == inputs.size &&
    inputs.map(_._2).zip(actualArgumentsType).forall { case (except, actual) => except == actual }
  }
}
