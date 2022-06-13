import com.typesafe.scalalogging.LazyLogging
import org.grapheco.tudb.ldbc.performance.PerformanceRunner
import org.junit.Test

import java.io.File

/** @program: TuDB-Embedded
  * @description:
  * @author: LiamGao
  * @create: 2022-03-25 15:58
  */
class PerformanceTest extends LazyLogging {
  @Test
  def performanceRunner(): Unit = {
    val file = new File("/home/tudb/ldbc/ldbc1.db")
    if (file.exists()) {
      val runner = new PerformanceRunner(file.getAbsolutePath, true)
      runner.run()
    } else
      logger.info(s"${file.getAbsolutePath} not exist, skip performance test.")
  }
}
