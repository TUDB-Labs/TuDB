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

package org.grapheco.tudb.ldbc.performance

import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.TimeUnit

/** @program: TuDB-Embedded
  * @description:
  * @author: LiamGao
  * @create: 2022-03-24 17:54
  */
object Tools extends LazyLogging {
  def printTimeCalculateResult(timeCostArray: Array[Long], apiName: String): Unit = {
    val costArray = timeCostArray.sorted.slice(1, timeCostArray.length - 1)
    val usTimeCost = costArray.sum / (costArray.length)
    val msTimeCost = TimeUnit.NANOSECONDS.toMillis(usTimeCost)
    logger.info(
      s"$apiName test ${costArray.length} times, avg cost $usTimeCost us ($msTimeCost ms)"
    )
  }

}
