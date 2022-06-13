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
