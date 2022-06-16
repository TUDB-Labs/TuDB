import org.apache.commons.io.FileUtils
import org.grapheco.tudb.TuInstanceContext
import org.grapheco.tudb.importer.TuImporter
import org.grapheco.tudb.store.meta.{DBNameMap, TuDBStatistics}
import org.grapheco.tudb.store.node.NodeStoreAPI
import org.grapheco.tudb.store.relationship.RelationshipStoreAPI
import org.grapheco.tudb.store.storage.RocksDBStorage
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

  var nodeStore: NodeStoreAPI = _
  var relationshipStore: RelationshipStoreAPI = _

  @Before
  def init(): Unit = {
    val f = new File(_dbPath)
    if (f.exists()) FileUtils.deleteDirectory(f)
    TuInstanceContext.setDataPath(_dbPath)
  }

  @After
  def clean(): Unit = {
    nodeStore.close()
    relationshipStore.close()
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
      Array(commentDataPath1, commentDataPath2, personDataPath, relationDataPath1, relationDataPath2)

    TuImporter.main(script)

    val statisticDB = new TuDBStatistics(_dbPath)
    statisticDB.init()

    val nodeMetaDB = RocksDBStorage.getDB(s"${_dbPath}/${DBNameMap.nodeMetaDB}")
    nodeStore = new NodeStoreAPI(
      s"${_dbPath}/${DBNameMap.nodeDB}",
      "default",
      s"${_dbPath}/${DBNameMap.nodeLabelDB}",
      "default",
      nodeMetaDB,
      "hashmap://mem"
    )
    Assert.assertEquals(862, nodeStore.allNodes().size)
    Assert.assertEquals(862, statisticDB.nodeCount)

    val relationMetaDB =
      RocksDBStorage.getDB(s"${_dbPath}/${DBNameMap.relationMetaDB}")
    relationshipStore = new RelationshipStoreAPI(
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
    Assert.assertEquals(821, relationshipStore.allRelations().size)
    Assert.assertEquals(821, statisticDB.relationCount)
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

    val r1 = relationshipStore.getRelationById(1961).get
    val rTypeId = relationshipStore.getRelationTypeId("hasCreator").get
    val rPropId = relationshipStore.getPropertyKeyId("creationDate").get
    Assert.assertTrue(rTypeId == r1.typeId)
    Assert.assertTrue(r1.properties.keySet == Set(rPropId))
    Assert.assertEquals(801030792151558L, r1.from)
    Assert.assertEquals(200000000000016L, r1.to)
  }
}
