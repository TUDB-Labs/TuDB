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

package org.grapheco.tudb.ldbc.performance

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.tudb.store.meta.DBNameMap
import org.grapheco.tudb.store.node.NodeStoreAPI
import org.grapheco.tudb.store.relationship.RelationshipStoreAPI
import org.grapheco.tudb.store.storage.{KeyValueDB, RocksDBStorage}
import org.grapheco.tudb.TuDBStoreContext

/** @program: TuDB-Embedded
  * @description:
  * @author: LiamGao
  * @create: 2022-03-24 16:53
  */
class PerformanceRunner(dbPath: String, testScanAllNodeData: Boolean) extends LazyLogging {
  private var nodeMetaDB: KeyValueDB = _
  private var relationMetaDB: KeyValueDB = _

  private def initDB(): Unit = {
    nodeMetaDB = RocksDBStorage.getDB(s"${dbPath}/${DBNameMap.nodeMetaDB}")
    relationMetaDB = RocksDBStorage.getDB(s"${dbPath}/${DBNameMap.relationMetaDB}")

    TuDBStoreContext.initializeNodeStoreAPI(
      s"${dbPath}/${DBNameMap.nodeDB}",
      "default",
      s"${dbPath}/${DBNameMap.nodeLabelDB}",
      "default",
      nodeMetaDB,
      "tudb://index?type=dummy",
      dbPath
    )
    TuDBStoreContext.initializeRelationshipStoreAPI(
      s"${dbPath}/${DBNameMap.relationDB}",
      "default",
      s"${dbPath}/${DBNameMap.inRelationDB}",
      "default",
      s"${dbPath}/${DBNameMap.outRelationDB}",
      "default",
      s"${dbPath}/${DBNameMap.relationLabelDB}",
      "default",
      relationMetaDB
    )
  }

  def run(): Unit = {
    logger.info(
      "============================= START PERFORMANCE TEST ============================="
    )
    println()
    initDB()
    val nodePerformance = new NodePerformance(TuDBStoreContext.getNodeStoreAPI, testScanAllNodeData)
    val relationPerformance = new RelationPerformance(TuDBStoreContext.getRelationshipAPI)

    logger.info(
      "============================= Start [NodeStoreAPI] Test ============================="
    )
    nodePerformance.run()

    logger.info(
      "============================= Start [RelationStoreAPI] Test ============================="
    )
    relationPerformance.run()

    closeDB()
    logger.info(
      "============================= FINISH PERFORMANCE TEST ============================="
    )
  }

  private def closeDB(): Unit = {
    TuDBStoreContext.getNodeStoreAPI.close()
    TuDBStoreContext.getRelationshipAPI.close()
  }
}
