package org.grapheco.tudb.graph

import collection.mutable.Map

import org.junit._

class RelationshipTest {

  @Test
  def loadDumpsTest() = {
    val relationshipId = 23
    val from = 0
    val to = 1
    val typeId = 3
    val properties = Map((1, 2), (2, "3"))
    val relationship = new Relationship(relationshipId, from, to, typeId, properties)
    relationship.addProperty(3, "4")
    val bytes = relationship.dumps()
    val parsedRelationship = Relationship.loads(bytes)
    Assert.assertEquals(relationship.property(3), parsedRelationship.property(3))
  }

  @Test
  def propertyTest() = {
    val relationshipId = 23
    val from = 0
    val to = 1
    val typeId = 3
    val properties = Map((1, 2), (2, "3"))
    val relationship = new Relationship(relationshipId, from, to, typeId, properties)
    relationship.addProperty(3, "4")
    Assert.assertEquals("4", relationship.property(3).get)
  }
}
