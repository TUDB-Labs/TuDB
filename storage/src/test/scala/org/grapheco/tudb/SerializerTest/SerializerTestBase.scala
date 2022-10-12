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

package org.grapheco.tudb.SerializerTest

import org.junit.Assert

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 9:20 下午 2022/1/26
  * @Modified By: LianxinGao
  */
class SerializerTestBase {
  def propsEqual(map1: Map[Int, Any], map2: Map[Int, Any]): Unit = {
    Assert.assertEquals(map1.size, map2.size)

    map1.foreach(kv => {
      kv._2 match {
        case array: Array[_] =>
          Assert.assertTrue(
            array sameElements map2(kv._1).asInstanceOf[Array[_]]
          )
        case _ => Assert.assertEquals(kv._2, map2(kv._1))
      }
    })
  }
}
