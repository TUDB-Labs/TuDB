package org.grapheco.tudb

import org.grapheco.tudb.store.node.NodeStoreAPI
import org.grapheco.tudb.store.relationship.RelationshipStoreAPI

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 21:19 2022/4/12
  * @Modified By:
  */
object TuStoreContext extends ContextMap {

  def setNodeStoreAPI(nodeStoreAPI: NodeStoreAPI) =
    super.put("nodeStoreAPI", nodeStoreAPI)
  def getNodeStoreAPI: NodeStoreAPI = super.get("nodeStoreAPI")

  def setRelationshipAPI(relationshipStoreAPI: RelationshipStoreAPI) =
    super.put("relationshipStoreAPI", relationshipStoreAPI)
  def getRelationshipAPI: RelationshipStoreAPI =
    super.get("relationshipStoreAPI")
}
