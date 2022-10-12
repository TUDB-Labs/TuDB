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
