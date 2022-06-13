package org.grapheco.tudb.store.meta

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 9:02 下午 2022/2/1
  * @Modified By:
  */
object DBNameMap {
  val nodeDB = "nodeDB"
  val nodeLabelDB = "nodeLabel"
  val nodeMetaDB = "nodeMeta"
  val relationDB = "relationshipDB"
  val inRelationDB = "inRelation"
  val outRelationDB = "outRelation"
  val relationLabelDB = "relationLabel"
  val relationMetaDB = "relationMeta"
  val statisticsDB = "statistics"
  val indexDB = "indexDB"
  val indexMetaDB = "indexMeta"

  val undoLogName = "undoLog.txt"
  val guardLogName = "guardLog.txt"
}
