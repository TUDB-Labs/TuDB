/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.test.server

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxRelationshipType}
import org.grapheco.tudb.graph.{TuNode, TuRelationship}
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

    val node = TuNode(
      1L,
      List[LynxNodeLabel](LynxNodeLabel("name")),
      List[(String, LynxValue)](("name") -> LynxString("sd"))
    )
    val json = node.toJson()
    println(json)
    Assert.assertTrue(json == """{"identity":1,"labels":["name"],"properties":{"name":"sd"}}""")
    val relation = TuRelationship(
      5L,
      1L,
      2L,
      Some(LynxRelationshipType("a")),
      List(("year") -> LynxString("2200"))
    )
    val json2 = relation.toJson()
    println(json2)
    Assert.assertTrue(
      json2 == """{"identity":5,"start":1,"end":2,"type":"a","properties":{"year":"2200"}}"""
    )
    val list_data=List(node,relation)
    val json3=list_data.toJson()
    println(json3)
    val map_data=Map("a"->node,"b"->relation)
    val json4=map_data.toJson()
    println(json4)
  }

  @Before
  def cleanUp(): Unit = {}

  @After
  def close(): Unit = {}

}
