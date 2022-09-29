package org.grapheco.tudb.engine

import org.grapheco.tudb.ContextMap
import org.grapheco.tudb.store.storage.KeyValueDB

class OperationsContext extends ContextMap {

  def setNodeDB(db: KeyValueDB) = super.put("__node_db__", db)
  def getNodeDB() = super.get("__node_db__")

  def setRelationshipDB(db: KeyValueDB) = super.put("__relationship_db__", db)
  def getRelationshipDB() = super.get("__relationship_db__")

  def setIndexDB(db: KeyValueDB) = super.put("__index_db__", db)
  def getIndexDB() = super.get("__index_db__")

  def setMetaDB(db: KeyValueDB) = super.put("__meta_db__", db)
  def getMetaDB() = super.get("__meta_db__")

}
