package org.grapheco.tudb

import com.typesafe.scalalogging.StrictLogging
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.store.meta.{DBNameMap, TuDBStatistics}
import org.grapheco.tudb.store.node.NodeStoreAPI
import org.grapheco.tudb.store.relationship.RelationshipStoreAPI
import org.grapheco.tudb.store.storage.RocksDBStorage

import java.io.File

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 12:36 2022/4/9
  * @Modified By:
  */
object GraphDatabaseBuilder extends StrictLogging {

  def newEmbeddedDatabase(
      dataPath: String,
      indexUri: String,
      rocksdbConfPath: String = "default"
  ): GraphFacade = {
    val file = new File(dataPath)
    if (!file.exists()) {
      file.mkdirs()
      logger.info(s"New created data path (${dataPath})")
    } else {
      if (file.isFile) {
        throw new Exception(
          s"The data path (${dataPath}) is invalid: not directory"
        )
      }
    }

    val nodeMetaDB =
      RocksDBStorage.getDB(s"${dataPath}/${DBNameMap.nodeMetaDB}")
    val nodeStoreAPI = new NodeStoreAPI(
      s"${dataPath}/${DBNameMap.nodeDB}",
      "default",
      s"${dataPath}/${DBNameMap.nodeLabelDB}",
      "default",
      nodeMetaDB,
      indexUri,
      dataPath
    )

    val relMetaDB =
      RocksDBStorage.getDB(s"${dataPath}/${DBNameMap.relationMetaDB}")
    val relationStoreAPI = new RelationshipStoreAPI(
      s"${dataPath}/${DBNameMap.relationDB}",
      "default",
      s"${dataPath}/${DBNameMap.inRelationDB}",
      "default",
      s"${dataPath}/${DBNameMap.outRelationDB}",
      "default",
      s"${dataPath}/${DBNameMap.relationLabelDB}",
      "default",
      relMetaDB
    )

    TuStoreContext.setNodeStoreAPI(nodeStoreAPI)
    TuStoreContext.setRelationshipAPI(relationStoreAPI)
    val statistics = new TuDBStatistics(dataPath, rocksdbConfPath)
    statistics.init()
    new GraphFacade(
      nodeStoreAPI,
      relationStoreAPI,
      statistics,
      {}
    )
  }

  private def checkDir(dir: String): Unit = {
    val file = new File(dir)
    if (!file.exists()) {
      file.mkdirs()
      logger.info(s"New created data path (${dir})")
    } else {
      if (!file.isDirectory) {
        throw new Exception(s"The data path (${dir}) is invalid: not directory")
      }
    }
  }

  private def assureFileExist(path: String): Unit = {
    val file = new File(path)
    if (!file.exists()) {
      throw new Exception(s"Can not find file (${path}) !")
    } else if (!file.isFile) {
      throw new Exception(s"The file (${path}) is invalid: not file")
    }
  }
}
