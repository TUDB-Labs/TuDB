/** Copyright (c) 2022 PandaDB **/
package org.grapheco.lynx

import org.junit.Test

class TMPTest extends TestBase {

  @Test
  def testMethod(): Unit ={
    runOnDemoGraph("create (n:City{name:'ChengDu'})") // id = 1
    runOnDemoGraph("create (n:Tag{name:'T1'})") // id = 2
    runOnDemoGraph("create (n:Tag{name:'T2'})") // id = 3
    runOnDemoGraph("create (n:Tag{name:'T3'})") // id = 4
    runOnDemoGraph("create (n:Organisation{name:'CCTV'})") // id = 5
    runOnDemoGraph(s"""
                      |MATCH (c:City) where c.id=1
                      |CREATE (p:Person {
                      |    id: 6,
                      |    firstName: 'A',
                      |    lastName: 'B',
                      |    gender: 'C',
                      |    birthday: 'D',
                      |    creationDate: 'E',
                      |    locationIP: 'F',
                      |    browserUsed: 'G',
                      |    languages: 'H',
                      |    email: 'I'
                      |  })-[:IS_LOCATED_IN]->(c)
                      |WITH p, count(*) AS dummy1
                      |UNWIND [2, 3, 4] AS tagId
                      |  MATCH (t:Tag {id: tagId})
                      |  CREATE (p)-[:HAS_INTEREST]->(t)
                      |WITH p, count(*) AS dummy2
                      |UNWIND [5, 2020] AS s
                      |  MATCH (u:Organisation {id: s[0]})
                      |  CREATE (p)-[:STUDY_AT {classYear: s[1]}]->(u)
                      |WITH p, count(*) AS dummy3
                      |UNWIND [5, 2000] AS w
                      |  MATCH (comp:Organisation {id: w[0]})
                      |  CREATE (p)-[:WORKS_AT {workFrom: w[1]}]->(comp)""".stripMargin)
    // TODO 可能没创建起 也可能没查出来
    runOnDemoGraph("match (n)-[r]->(m) return r").show()
    Thread.sleep(1000*1000)

  }


}
