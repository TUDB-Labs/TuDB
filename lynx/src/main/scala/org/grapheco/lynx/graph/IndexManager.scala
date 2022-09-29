package org.grapheco.lynx.graph

/** Manage index creation and deletion.
  */
trait IndexManager {

  /** Create Index.
    * @param index The index to create
    */
  def createIndex(index: Index): Unit

  /** Drop Index.
    * @param index The index to drop
    */
  def dropIndex(index: Index): Unit

  /** Get all indexes.
    * @return all indexes
    */
  def indexes: Array[Index]
}
