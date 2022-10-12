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
