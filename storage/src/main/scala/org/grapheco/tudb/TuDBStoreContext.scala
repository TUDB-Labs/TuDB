package org.grapheco.tudb

import org.grapheco.tudb.store.node.NodeStoreAPI
import org.grapheco.tudb.store.relationship.RelationshipStoreAPI
import org.grapheco.tudb.store.storage.KeyValueDB

class TuDBStoreContext extends ContextMap {

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
