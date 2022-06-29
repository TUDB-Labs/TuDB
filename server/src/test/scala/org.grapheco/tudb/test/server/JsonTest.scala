/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.test.server

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.LynxNodeLabel
import org.grapheco.tudb.graph.TuNode
import org.junit._
import org.junit.runners.MethodSorters

/** @Author: huanglin
 * @Description:
 * @Date: Created at 2022-6-29
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class JsonTest {

  @Test
  def testQueryReturnJson(): Unit = {
    import org.grapheco.tudb.TuDBJsonTool.AnyRefAddMethod

    val node=TuNode(1L,List[LynxNodeLabel](LynxNodeLabel("name")),
      List[(String,LynxValue)](("name")->LynxString("sd")))
    val json=node.toJson()
    println(json)
    Assert.assertTrue(json=="""{"identity":1,"labels":[{"value":"name"}],"properties":{"name":"sd"}}""")
  }


  @Before
  def cleanUp(): Unit = {

  }

  @After
  def close(): Unit = {

  }


}
