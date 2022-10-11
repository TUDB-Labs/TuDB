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

package org.grapheco.tudb

import com.typesafe.scalalogging.StrictLogging
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.store.meta.{DBNameMap, TuDBStatistics}
import org.grapheco.tudb.store.node.NodeStoreAPI
import org.grapheco.tudb.store.relationship.RelationshipStoreAPI
import org.grapheco.tudb.store.storage.RocksDBStorage
import org.grapheco.tudb.TuDBStoreContext

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
    TuDBStoreContext.initializeNodeStoreAPI(
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
    TuDBStoreContext.initializeRelationshipStoreAPI(
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

    val statistics = new TuDBStatistics(dataPath, rocksdbConfPath)
    statistics.init()
    new GraphFacade(
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
