package org.grapheco.tudb.ldbc.performance

import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent.TimeUnit

/** @program: TuDB-Embedded
  * @description:
  * @author: LiamGao
  * @create: 2022-03-24 17:54
  */
object Tools extends LazyLogging {
  def printTimeCalculateResult(
      timeCostArray: Array[Long],
      apiName: String
  ): Unit = {
    val costArray = timeCostArray.sorted.slice(1, timeCostArray.length - 1)
    val usTimeCost = costArray.sum / (costArray.length)
    val msTimeCost = TimeUnit.NANOSECONDS.toMillis(usTimeCost)
    logger.info(
      s"$apiName test ${costArray.length} times, avg cost $usTimeCost us ($msTimeCost ms)"
    )
  }

}
