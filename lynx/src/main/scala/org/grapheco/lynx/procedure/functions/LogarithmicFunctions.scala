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

package org.grapheco.lynx.procedure.functions

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxFloat, LynxNumber}

/** @ClassName LogarithmicFunctions
  * @Description These functions all operate on numerical expressions only,
  * and will return an error if used on any other values.
  * @Author huchuan
  * @Date 2022/4/20
  * @Version 0.1
  */
class LogarithmicFunctions {
  @LynxProcedure(name = "e")
  def e(args: Seq[LynxValue]): LynxNumber = {
    LynxFloat(Math.E)
  }

  @LynxProcedure(name = "exp")
  def exp(args: Seq[LynxNumber]): LynxNumber = {
    LynxFloat(math.exp(args.head.number.doubleValue()))
  }

  @LynxProcedure(name = "log")
  def log(args: Seq[LynxNumber]): LynxNumber = {
    LynxFloat(math.log(args.head.number.doubleValue()))
  }

  @LynxProcedure(name = "log10")
  def log10(args: Seq[LynxNumber]): LynxNumber = {
    LynxFloat(math.log10(args.head.number.doubleValue()))
  }

  @LynxProcedure(name = "sqrt")
  def sqrt(args: Seq[LynxNumber]): LynxNumber = {
    LynxFloat(math.sqrt(args.head.number.doubleValue()))
  }
}
