package org.grapheco.tudb.ldbc.performance

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 11:05 2022/3/22
  * @Modified By:
  */
object TestEntry {
  def main(args: Array[String]): Unit = {
    val dbPath = args(0)
    val testScanAllNodes = args(1).toBoolean
    val rp = new PerformanceRunner(dbPath, testScanAllNodes)
    rp.run()
  }
}
