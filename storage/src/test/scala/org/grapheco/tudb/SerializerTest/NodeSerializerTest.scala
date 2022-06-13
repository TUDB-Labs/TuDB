package org.grapheco.tudb.SerializerTest

import org.grapheco.tudb.serializer.{BaseSerializer, NodeSerializer, SpecialID}
import org.grapheco.tudb.store.node.StoredNodeWithProperty
import org.junit.{Assert, Test}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 7:34 下午 2022/1/26
  * @Modified By: LianxinGao
  */
class NodeSerializerTest extends SerializerTestBase {
  val id1 = 1L
  val id2 = 2L
  val labelIDs1 = Array(1)
  val labelIDs2 = Array(1, 2, 3)
  val props1: Map[Int, Any] = Map(
    1 -> 1,
    2 -> 2L,
    3 -> 3.0,
    4 -> "4.00",
    5 -> true,
    6 -> Array(1, 2.0, "3.00", false)
  )
  val props2: Map[Int, Any] = Map(
    7 -> 5,
    8 -> 6.0f,
    9 -> "7.0",
    10 -> 8.0,
    11 -> Array(3, 2.0, "1.00", true)
  )
  val serializedNode1 =
    NodeSerializer.encodeNodeWithProperties(id1, labelIDs1, props1)
  val serializedNode2 =
    NodeSerializer.encodeNodeWithProperties(id2, labelIDs2, props2)

  @Test
  def encodeNodeKeyTest(): Unit = {
    val key1 = NodeSerializer.encodeNodeKey(id1, labelIDs1.head)
    val key2 = NodeSerializer.encodeNodeKey(id2, labelIDs2)
    val k1 = labelIDs1.map(f => (id1, f))
    val k2 = labelIDs2.map(f => (id2, f))

    Assert.assertTrue(Array(NodeSerializer.decodeNodeKey(key1)) sameElements k1)
    Assert.assertTrue(
      key2.map(k => NodeSerializer.decodeNodeKey(k)) sameElements k2
    )
  }

  @Test
  def encodeNodeLabelTest(): Unit = {
    val el = NodeSerializer.encodeNodeLabels(labelIDs1)
    val res = NodeSerializer.decodeNodeLabelIds(el)
    Assert.assertArrayEquals(res, labelIDs1)
  }

  @Test
  def encodeNodeLabelKeyTest(): Unit = {
    val labelKey1 = NodeSerializer.encodeNodeLabelKey(1, 233)
    val lk1 = NodeSerializer.decodeLabelIdInNodeLabelKey(labelKey1)
    val lk2 = NodeSerializer.decodeNodeIdInNodeLabelKey(labelKey1)
    Assert.assertEquals(lk1, 233)
    Assert.assertEquals(lk2, 1)

    val labelKey2 = NodeSerializer.encodeNodeLabelKey(666)
    val lk3 = NodeSerializer.decodeLabelIdInNodeLabelKey(labelKey2)
    val lk4 = NodeSerializer.decodeNodeIdInNodeLabelKey(labelKey2)
    Assert.assertEquals(lk3, SpecialID.NONE_LABEL_ID)
    Assert.assertEquals(lk4, 666)
  }

  @Test
  def encodeNodePropertiesTest(): Unit = {
    val p = NodeSerializer.encodeNodeProperties(props1)
    val dp = BaseSerializer.decodePropMap(p)
    propsEqual(dp, props1)
  }

  @Test
  def encodeNodeWithPropertiesTest(): Unit = {

    val node1 = new StoredNodeWithProperty(233, Array(1, 2, 3), props1)
    val encode1 = NodeSerializer.encodeNodeWithProperties(node1)
    val decode1_node = NodeSerializer.decodeNodeWithProperties(encode1)
    val decode1_prop = NodeSerializer.decodePropertiesFromFullNode(encode1)
    Assert.assertEquals(decode1_node.id, node1.id)
    Assert.assertArrayEquals(decode1_node.labelIds, node1.labelIds)
    propsEqual(node1.properties, decode1_prop)

    val node2 = new StoredNodeWithProperty(123, Array(2), props2)
    val encode2 = NodeSerializer.encodeNodeWithProperties(123, Array(2), props2)
    val decode2_node = NodeSerializer.decodeNodeWithProperties(encode2)
    val decode2_prop = NodeSerializer.decodePropertiesFromFullNode(encode2)
    Assert.assertEquals(decode2_node.id, node2.id)
    Assert.assertArrayEquals(decode2_node.labelIds, node2.labelIds)
    propsEqual(node2.properties, decode2_prop)
  }

  @Test
  def decodePropertiesFromFullNodeTest(): Unit = {
    val prop = NodeSerializer.decodePropertiesFromFullNode(serializedNode1)
    propsEqual(prop, props1)
  }

  @Test
  def testSingleNode(): Unit = {
    val node1 = NodeSerializer.decodeNodeWithProperties(serializedNode1)
    Assert.assertEquals(id1, node1.id)
    Assert.assertArrayEquals(labelIDs1, node1.labelIds)
    Assert.assertEquals(node1.properties.size, props1.size)
    propsEqual(node1.properties, props1)
  }

  @Test
  def testMultiNodes(): Unit = {
    val node1 = NodeSerializer.decodeNodeWithProperties(serializedNode1)
    val node2 = NodeSerializer.decodeNodeWithProperties(serializedNode2)
    Assert.assertEquals(id1, node1.id)
    Assert.assertArrayEquals(labelIDs1, node1.labelIds)
    propsEqual(node1.properties, props1)

    Assert.assertEquals(id2, node2.id)
    Assert.assertArrayEquals(labelIDs2, node2.labelIds)
    propsEqual(node2.properties, props2)
  }
}
