/** Copyright (c) 2022 TuDB * */
package org.grapheco.tudb.test.server

import org.grapheco.lynx.LynxResult
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxRelationshipType}
import org.grapheco.lynx.util.FormatUtils
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
    val list_data = List(node, relation)
    val json3 = list_data.toJson()
    println(json3)
    Assert.assertTrue(
      json3 == """[{"identity":1,"labels":["name"],"properties":{"name":"sd"}},{"identity":5,"start":1,"end":2,"type":"a","properties":{"year":"2200"}}]"""
    )
    val map_data = Map("a" -> node, "b" -> relation)
    val json4 = map_data.toJson()
    println(json4)
    Assert.assertTrue(
      json4 == """{"a":{"identity":1,"labels":["name"],"properties":{"name":"sd"}},"b":{"identity":5,"start":1,"end":2,"type":"a","properties":{"year":"2200"}}}"""
    )

    val resutlData = new LynxResult {
      override def show(limit: Int): Unit = println(limit)

      override def cache(): LynxResult = this

      override def columns(): Seq[String] = List("a", "b")

      override def records(): Iterator[Map[String, LynxValue]] =
        Iterator(Map("a" -> LynxString("1"), "b" -> LynxString("2")))

    }
    val json5 = resutlData.toJson()
    println(json5)
    Assert.assertTrue(
      json5 == """[[{"keys": ["a"],"length": 1,"_fields":["1"]},{"keys": ["b"],"length": 1,"_fields":["2"]}]]"""
    )
  }

  @Before
  def cleanUp(): Unit = {}

  @After
  def close(): Unit = {}

}
