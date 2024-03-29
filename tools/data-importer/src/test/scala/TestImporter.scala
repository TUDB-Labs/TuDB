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

import org.apache.commons.io.FileUtils
import org.grapheco.tudb.TuDBInstanceContext
import org.grapheco.tudb.importer.TuImporter
import org.grapheco.tudb.store.meta.{DBNameMap, TuDBStatistics}
import org.grapheco.tudb.store.node.NodeStoreAPI
import org.grapheco.tudb.store.relationship.RelationshipStoreAPI
import org.grapheco.tudb.store.storage.RocksDBStorage
import org.grapheco.tudb.TuDBStoreContext
import org.junit.{After, Assert, Before, Test}

import java.io.File

/** @program: TuDB-Embedded
  * @description:
  * @author: LiamGao
  * @create: 2022-03-17 17:30
  */
class TestImporter {
  def getRootPath = System.getProperty("user.dir")
  val _dbPath = s"${getRootPath}/testImporter/test.db"

  @Before
  def init(): Unit = {
    val f = new File(_dbPath)
    if (f.exists()) FileUtils.deleteDirectory(f)
    TuDBInstanceContext.setDataPath(_dbPath)
  }

  @After
  def clean(): Unit = {
    TuDBStoreContext.getNodeStoreAPI.close()
    TuDBStoreContext.getRelationshipAPI.close()
    val f = new File(_dbPath)
    if (f.exists()) FileUtils.deleteDirectory(f)
  }

  @Test
  def testImportData(): Unit = {
    var script =
      Array(s"--db-path=${_dbPath}", s"--delimeter=|", s"--array-delimeter=,")
    val commentDataPath1 =
      "--nodes=" + getClass.getResource("nodes/comment.csv").getPath
    val commentDataPath2 =
      "--nodes=" + getClass.getResource("nodes/comment2.csv").getPath

    val personDataPath =
      "--nodes=" + getClass.getResource("nodes/person.csv").getPath
    val relationDataPath1 = "--relationships=" + getClass
      .getResource("relationships/comment_hasCreator_person.csv")
      .getPath
    val relationDataPath2 = "--relationships=" + getClass
      .getResource("relationships/comment_hasCreator_person2.csv")
      .getPath
    script = script ++
      Array(
        commentDataPath1,
        commentDataPath2,
        personDataPath,
        relationDataPath1,
        relationDataPath2
      )

    TuImporter.main(script)

    val statisticDB = new TuDBStatistics(_dbPath)
    statisticDB.init()

    val nodeMetaDB = RocksDBStorage.getDB(s"${_dbPath}/${DBNameMap.nodeMetaDB}")
    TuDBStoreContext.initializeNodeStoreAPI(
      s"${_dbPath}/${DBNameMap.nodeDB}",
      "default",
      s"${_dbPath}/${DBNameMap.nodeLabelDB}",
      "default",
      nodeMetaDB,
      "tudb://index?type=dummy",
      _dbPath
    )
    Assert.assertEquals(862, TuDBStoreContext.getNodeStoreAPI.allNodes().size)
    Assert.assertEquals(862, statisticDB.nodeCount)

    val relationMetaDB =
      RocksDBStorage.getDB(s"${_dbPath}/${DBNameMap.relationMetaDB}")
    TuDBStoreContext.initializeRelationshipStoreAPI(
      s"${_dbPath}/${DBNameMap.relationDB}",
      "default",
      s"${_dbPath}/${DBNameMap.inRelationDB}",
      "default",
      s"${_dbPath}/${DBNameMap.outRelationDB}",
      "default",
      s"${_dbPath}/${DBNameMap.relationLabelDB}",
      "default",
      relationMetaDB
    )
    Assert.assertEquals(821, TuDBStoreContext.getRelationshipAPI.allRelations().size)
    Assert.assertEquals(821, statisticDB.relationCount)
    val nodeStore: NodeStoreAPI = TuDBStoreContext.getNodeStoreAPI
    // meta test
    val node = nodeStore.getNodeById(801030792151560L).get
    val labelId = nodeStore.getLabelId("comment").get
    val propId0 = nodeStore.getPropertyKeyId("id").get
    val propId1 = nodeStore.getPropertyKeyId("locationIP").get
    val propId2 = nodeStore.getPropertyKeyId("browserUsed").get
    val propId3 = nodeStore.getPropertyKeyId("content").get
    val propId4 = nodeStore.getPropertyKeyId("length").get
    val propId5 = nodeStore.getPropertyKeyId("creationDate").get
    Assert.assertTrue(node.labelIds.sameElements(Array(labelId)))
    Assert.assertTrue(
      node.properties.keySet == Set(
        propId0,
        propId1,
        propId2,
        propId3,
        propId4,
        propId5
      )
    )

    val relationshipStore: RelationshipStoreAPI = TuDBStoreContext.getRelationshipAPI

    val r1 = relationshipStore.getRelationById(1961).get
    val rTypeId = relationshipStore.getRelationTypeId("hasCreator").get
    val rPropId = relationshipStore.getPropertyKeyId("creationDate").get
    Assert.assertTrue(rTypeId == r1.typeId)
    Assert.assertTrue(r1.properties.keySet == Set(rPropId))
    Assert.assertEquals(801030792151558L, r1.from)
    Assert.assertEquals(200000000000016L, r1.to)
  }
}
