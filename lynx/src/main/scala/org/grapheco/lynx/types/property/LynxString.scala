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

package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.util.symbols.{CTString, StringType}

/**
  * @ClassName LynxString
  * @Description TODO
  * @Author huchuan
  * @Date 2022/4/1
  * @Version 0.1
  */
case class LynxString(v: String) extends LynxValue {
  def value: String = v

  def lynxType: StringType = CTString

  override def >(lynxValue: LynxValue): Boolean =
    this.value > lynxValue.asInstanceOf[LynxString].value

  override def >=(lynxValue: LynxValue): Boolean =
    this.value >= lynxValue.asInstanceOf[LynxString].value

  override def <(lynxValue: LynxValue): Boolean =
    this.value < lynxValue.asInstanceOf[LynxString].value

  override def <=(lynxValue: LynxValue): Boolean =
    this.value <= lynxValue.asInstanceOf[LynxString].value
}