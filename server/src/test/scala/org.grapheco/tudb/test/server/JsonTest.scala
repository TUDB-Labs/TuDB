/** Copyright (c) 2022 TuDB * */
package org.grapheco.tudb.test.server


import org.grapheco.lynx.{LynxResult, PathTriple}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxPath, LynxString}
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
    
    /**  one-hop test
     */
    val node3 = TuNode(
      2L,
      List[LynxNodeLabel](LynxNodeLabel("name")),
      List[(String, LynxValue)](("name") -> LynxString("sf"))
    )
    val oneHop = LynxPath(Seq(PathTriple(node, relation, node3)))
    val json5_1 = oneHop.toJson()
    println(json5_1)
    Assert.assertTrue(
      json5_1 == """{"start":{"identity":1,"labels":["name"],"properties":{"name":"sd"}},"end":{"identity":2,"labels":["name"],"properties":{"name":"sf"}},"segments":[{"start":{"identity":1,"labels":["name"],"properties":{"name":"sd"}},"end":{"identity":2,"labels":["name"],"properties":{"name":"sf"}},"relationship":{"identity":5,"start":1,"end":2,"type":"a","properties":{"year":"2200"}}}],"length":1}"""
    )

    /** two-hop test
     */
    val node4 = TuNode(
      3L,
      List[LynxNodeLabel](LynxNodeLabel("name")),
      List[(String, LynxValue)]("name" -> LynxValue("sg"))
    )
    val twoHop = LynxPath(
      Seq[PathTriple](PathTriple(node, relation, node3), PathTriple(node3, relation, node4))
    )
    val json6 = twoHop.toJson()
    println(json6)
    Assert.assertTrue {
      json6 == """{"start":{"identity":1,"labels":["name"],"properties":{"name":"sd"}},"end":{"identity":3,"labels":["name"],"properties":{"name":"sg"}},"segments":[{"start":{"identity":1,"labels":["name"],"properties":{"name":"sd"}},"end":{"identity":2,"labels":["name"],"properties":{"name":"sf"}},"relationship":{"identity":5,"start":1,"end":2,"type":"a","properties":{"year":"2200"}}},{"start":{"identity":2,"labels":["name"],"properties":{"name":"sf"}},"end":{"identity":3,"labels":["name"],"properties":{"name":"sg"}},"relationship":{"identity":5,"start":1,"end":2,"type":"a","properties":{"year":"2200"}}}],"length":2}"""
    }

    /**
     *three-hop test
     */
    val node5 = TuNode(
      4L,
      List[LynxNodeLabel](LynxNodeLabel("name")),
      List[(String, LynxValue)]("name" -> LynxValue("sh"))
    )
    val threeHop = LynxPath(
      Seq[PathTriple](
        PathTriple(node, relation, node3),
        PathTriple(node3, relation, node4),
        PathTriple(node4, relation, node5)
      )
    )
    val json7 = threeHop.toJson()
    println(json7)
    Assert.assertTrue {
      json7 == """{"start":{"identity":1,"labels":["name"],"properties":{"name":"sd"}},"end":{"identity":4,"labels":["name"],"properties":{"name":"sh"}},"segments":[{"start":{"identity":1,"labels":["name"],"properties":{"name":"sd"}},"end":{"identity":2,"labels":["name"],"properties":{"name":"sf"}},"relationship":{"identity":5,"start":1,"end":2,"type":"a","properties":{"year":"2200"}}},{"start":{"identity":2,"labels":["name"],"properties":{"name":"sf"}},"end":{"identity":3,"labels":["name"],"properties":{"name":"sg"}},"relationship":{"identity":5,"start":1,"end":2,"type":"a","properties":{"year":"2200"}}},{"start":{"identity":3,"labels":["name"],"properties":{"name":"sg"}},"end":{"identity":4,"labels":["name"],"properties":{"name":"sh"}},"relationship":{"identity":5,"start":1,"end":2,"type":"a","properties":{"year":"2200"}}}],"length":3}"""
    }
  }

  @Before
  def cleanUp(): Unit = {}

  @After
  def close(): Unit = {}

}
