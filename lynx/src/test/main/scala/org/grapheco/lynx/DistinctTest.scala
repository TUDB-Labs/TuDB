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

import org.junit.{Assert, Test}

/**
  *@author:John117
  *@createDate:2022/7/18
  *@description:
  */
class DistinctTest extends TestBase {
  @Test
  def testDistinctRelationship(): Unit = {
    runOnDemoGraph("""
        |create (n1:Person{name:'A'})
        |create (n2:Person{name:'B'})
        |create (n3:Person{name:'C'})
        |create (n1)-[:KNOW]->(n3)
        |create (n2)-[:KNOW]->(n3)
        |""".stripMargin)

    var count = runOnDemoGraph("""
        |match (n)-[r:KNOW]->(m)
        |return count(m) as count
        |""".stripMargin).records().next()
    Assert.assertEquals(2L, count("count").value)

    count = runOnDemoGraph("""
        |match (n)-[r:KNOW]->(m)
        |with distinct m
        |return count(m) as count
        |""".stripMargin).records().next()
    Assert.assertEquals(1L, count("count").value)

    count = runOnDemoGraph("""
        |match (n)-[r:KNOW]->(m)
        |return count(m) as count
        |""".stripMargin).records().next()
    Assert.assertEquals(2L, count("count").value)

    count = runOnDemoGraph("""
        |match (n)-[r:KNOW]->(m)
        |return count(distinct m) as count
        |""".stripMargin).records().next()
    Assert.assertEquals(1L, count("count").value)
  }
}
