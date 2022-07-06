package org.grapheco.lynx;

import org.grapheco.lynx.procedure.exceptions.UnknownProcedureException
import org.grapheco.lynx.util.LynxDurationUtil
import org.grapheco.lynx.types.composite.LynxList
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxString}
import org.junit.function.ThrowingRunnable
import org.junit.{Assert, Test}

class CypherOptionMatchTest extends TestBase {
  runOnDemoGraph("""
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
  def testCypherOptionMatch(): Unit = {
    var rs = runOnDemoGraph(
      s"""
         | MATCH(n:person {name: "bluejoe"})
         | OPTIONAL MATCH (n)-->(x)
         | RETURN count(x)
         |""".stripMargin
    ).records().next()("count(x)")
    Assert.assertEquals(LynxInteger(2), rs)
  }

  @Test
  def testCypherOptionMatchNull(): Unit = {
    var rs = runOnDemoGraph(
      s"""
         | MATCH(n:person {name: "bluejoe"})
         | OPTIONAL MATCH (n)-[:LIKES]->(x)
         | RETURN count(x)
         |""".stripMargin
    ).records().next()("count(x)")
    Assert.assertEquals(LynxInteger(0), rs)
  }
}
