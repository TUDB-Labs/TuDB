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

import org.grapheco.tudb.store.node.NodeStoreAPI
import org.grapheco.tudb.store.relationship.RelationshipStoreAPI
import org.grapheco.tudb.store.storage.KeyValueDB

object TuDBStoreContext extends ContextMap {

  def initializeNodeStoreAPI(
      nodeDBPath: String,
      nodeDBConfigPath: String,
      nodeLabelDBPath: String,
      nodeLabelConfigPath: String,
      metaDB: KeyValueDB,
      indexUri: String,
      dbPath: String
    ): NodeStoreAPI = {
    val nodeStoreAPI = new NodeStoreAPI(
      nodeDBPath,
      nodeDBConfigPath,
      nodeLabelDBPath,
      nodeLabelConfigPath,
      metaDB,
      indexUri,
      dbPath
    )
    setNodeStoreAPI(nodeStoreAPI)
    nodeStoreAPI
  }

  def initializeRelationshipStoreAPI(
      relationDBPath: String,
      relationConfigPath: String,
      inRelationDBPath: String,
      inRelationConfigPath: String,
      outRelationDBPath: String,
      outRelationConfigPath: String,
      relationLabelDBPath: String,
      relationLabelConfigPath: String,
      metaDB: KeyValueDB
    ): RelationshipStoreAPI = {
    val relationshipStoreAPI = new RelationshipStoreAPI(
      relationDBPath,
      relationConfigPath,
      inRelationDBPath,
      inRelationConfigPath,
      outRelationDBPath,
      outRelationConfigPath,
      relationLabelDBPath,
      relationLabelConfigPath,
      metaDB
    )
    setRelationshipAPI(relationshipStoreAPI)
    relationshipStoreAPI
  }

  def setNodeStoreAPI(nodeStoreAPI: NodeStoreAPI) =
    super.put("nodeStoreAPI", nodeStoreAPI)

  def getNodeStoreAPI: NodeStoreAPI = super.get("nodeStoreAPI")

  def setRelationshipAPI(relationshipStoreAPI: RelationshipStoreAPI) =
    super.put("relationshipStoreAPI", relationshipStoreAPI)

  def getRelationshipAPI: RelationshipStoreAPI =
    super.get("relationshipStoreAPI")
}
