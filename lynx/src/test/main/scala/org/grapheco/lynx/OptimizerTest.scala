package org.grapheco.lynx;

import org.grapheco.lynx.procedure.exceptions.UnknownProcedureException
import org.grapheco.lynx.util.LynxDurationUtil
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxString}
import org.junit.function.ThrowingRunnable
import org.junit.{Assert, Test}

class OptimizerTest extends TestBase  {
  runOnDemoGraph(
    """
      |Create
      |(a:person:leader{name:"bluejoe", age: 40, gender:"male"}),
      |(b:person{name:"Alice", age: 30, gender:"female"}),
      |(c:person{name:"Bob", age: 10, gender:"male"}),
      |(d:person{name:"Bob2", age: 10, gender:"male"}),
      |(a)-[:KNOWS{years:5}]->(b),
      |(b)-[:KNOWS{years:4}]->(c),
      |(c)-[:KNOWS]->(d),
      |(a)-[]->(c)
      |""".stripMargin)
  @Test
  def testOptBugSimple(): Unit = {
    var rs = runOnDemoGraph(
      s"""
         | MATCH(n:person)
         | WITH n
         | MATCH(n1:leader)
         | WITH n, n1
         | WHERE 15 > n.age > 9 AND n1.gender in ["male"]
         | RETURN count(n)
         |""".stripMargin
    ).records().next()("count(n)")
    Assert.assertEquals(LynxInteger(2), rs)
  }
}
