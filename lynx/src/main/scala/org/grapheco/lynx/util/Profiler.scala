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

package org.grapheco.lynx.util

object Profiler {
  var enableTiming = false;

  def timing[T](runnable: => T): T =
    if (enableTiming) {
      val t1 = System.currentTimeMillis()
      var result: T = null.asInstanceOf[T];
      result = runnable

      val t2 = System.currentTimeMillis()

      println(new Exception().getStackTrace()(1).toString)

      val elapsed = t2 - t1;
      println(s"time cost: ${elapsed}ms")

      result
    } else {
      runnable
    }
}
