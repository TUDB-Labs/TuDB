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

package org.grapheco.lynx;

import org.grapheco.lynx.types.property.LynxInteger
import org.junit.{Assert, Test}

class OptimizerTest extends TestBase {
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

  @Test
  def testRepeatRelationship(): Unit = {
    val createQuery: String =
      s"""
         | create (AAA:Person {id:"1",
         | firstName: "AAA",
         | lastName: "BBB",
         | birthday: "2022-07-03 00:00:00.000",
         | locationIP: "localhost",
         | browserUsed: "safari",
         | gender: "man",
         | creationDate: "2022-07-03 00:00:00.000"}
         | )
         | create (BBB:Person {id:"22",
         | firstName: "BBB",
         | lastName: "CCC",
         | birthday: "2022-07-03 00:00:00.000",
         | locationIP: "localhost",
         | browserUsed: "safari",
         | gender: "man",
         | creationDate: "2022-07-04 00:00:00.000"}
         | )
         | create (CCC:Person {id:"23",
         | firstName: "CCC",
         | lastName: "DDD",
         | birthday: "2022-07-03 00:00:00.000",
         | locationIP: "localhost",
         | browserUsed: "safari",
         | gender: "man",
         | creationDate: "2022-07-05 00:00:00.000"}
         | )
         | create (DDD:Person {id:"24",
         | firstName: "DDD",
         | lastName: "EEE",
         | birthday: "2022-07-03 00:00:00.000",
         | locationIP: "localhost",
         | browserUsed: "safari",
         | gender: "man",
         | creationDate: "2022-07-05 00:00:00.000"}
         | )
         | create (AAA)-[:KNOWS {creationDate: "2022-07-31"}]->(BBB)
         | create (AAA)-[:KNOWS {creationDate: "2022-07-31"}]->(CCC)
         | create (AAA)-[:KNOWS {creationDate: "2022-06-21"}]->(DDD)
         |""".stripMargin
    runOnDemoGraph(createQuery)

    val matchQuery: String =
      s"""
         |MATCH (n:Person {id: "1" })-[r:KNOWS]-(friend)
         | RETURN
         | friend.id AS personId,
         | friend.firstName AS firstName,
         | friend.lastName AS lastName,
         | r.creationDate AS friendshipCreationDate
         | ORDER BY
         | friendshipCreationDate DESC,
         | toInteger(personId) ASC
         |""".stripMargin
    val size = runOnDemoGraph(matchQuery).records().length
    Assert.assertEquals(3, size)
  }
}
