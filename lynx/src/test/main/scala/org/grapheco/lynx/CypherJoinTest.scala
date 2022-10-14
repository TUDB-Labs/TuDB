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

package org.grapheco.lynx

import org.grapheco.lynx.types.property.LynxInteger
import org.junit.{Assert, Test}

class CypherJoinTest extends TestBase {

  runOnDemoGraph("""
      |Create
      |(a:person:leader{name:"bluejoe", age: 40, gender:"male"}),
      |(b:person{name:"Alice", age: 30, gender:"female"}),
      |(c{name:"Bob", age: 10, gender:"male"}),
      |(d{name:"Bob2", age: 10, gender:"male"}),
      |(a)-[:KNOWS{years:5}]->(b),
      |(b)-[:KNOWS{years:4}]->(c),
      |(c)-[:KNOWS]->(d),
      |(a)-[]->(c)
      |""".stripMargin)

  @Test
  def testMany2ManySingleMatch(): Unit = {
    val q = """MATCH
    (a:person),
    (b:person)
    RETURN count(a)""".stripMargin
    val rs = runOnDemoGraph(q).records().toSeq.head
    val a = rs.get("count(a)").get.asInstanceOf[LynxInteger]
    Assert.assertEquals(4, a.value)
  }

  @Test
  def testMany2ManyMultiMatch(): Unit = {
    val q = """MATCH
    (a:person)
    MATCH (b:person)
    RETURN count(a)""".stripMargin
    val rs = runOnDemoGraph(q).records().toSeq.head
    val a = rs.get("count(a)").get.asInstanceOf[LynxInteger]
    Assert.assertEquals(4, a.value)
  }
}
