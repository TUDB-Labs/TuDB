package org.grapheco.tudb.graph

import org.junit._
import collection.mutable.Set
import collection.mutable.Map

class NodeTest {

  @Test
  def propertyTest() = {
    val nodeId = 123
    val labelIds = Set(1, 2)
    val properties = Map((1, "2"), (2, 2))
    val node = new Node(nodeId, labelIds, properties)
    node.addLabelId(3)
    Assert.assertEquals("2", node.property(1).get.asInstanceOf[String])
    Assert.assertEquals(2, node.property(2).get.asInstanceOf[Int])
  }

  @Test
  def loadDumpTest() = {
    val nodeId = 123
    val labelIds = Set(1, 2, 3)
    val properties = Map((1, "2"), (3, 4))
    val node = new Node(nodeId, labelIds, properties)
    node.addProperty(2, "2")
    val bytes = node.dumps()
    val loadNode = Node.loads(bytes)
    Assert.assertEquals(node.property(2), loadNode.property(2))
  }
}
