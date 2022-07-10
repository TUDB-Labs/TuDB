/** Copyright (c) 2022 PandaDB **/
package org.grapheco.lynx

import org.junit.Test

class ReferenceTest extends TestBase {

  @Test
  def refer1(): Unit = {
    runOnDemoGraph("""
        |MATCH (countryX:Country {name: 'a' }),
        |        (countryY:Country {name: 'b' }),
        |        (person:Person {id: 1 })
        |        WITH person, countryX, countryY
        |        LIMIT 1
        |        MATCH (city:City)-[:IS_PART_OF]->(country:Country)
        |        WHERE country IN [countryX, countryY]
        |        return country
        |""".stripMargin)
  }
  @Test
  def refer2(): Unit = {
    runOnDemoGraph("""
        |match (n:Person)
        |match (m:Person) where m.city = n.city
        |return m
        |""".stripMargin)
  }

  @Test
  def refer3(): Unit = {
    runOnDemoGraph("""
        |unwind [1,2,3] as tagId
        |match (n: Tag) where id(n) = tagId
        |return n
        |""".stripMargin)
  }
  @Test
  def refer4(): Unit = {
    runOnDemoGraph("""
        |UNWIND [2,3,4] AS s
        |MATCH (u:Organisation {id: s[0]})
        |return u
        |""".stripMargin)
  }
}
