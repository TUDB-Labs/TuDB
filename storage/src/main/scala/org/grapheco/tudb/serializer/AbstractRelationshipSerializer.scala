package org.grapheco.tudb.serializer

import org.grapheco.tudb.store.relationship.{StoredRelationship, StoredRelationshipWithProperty}

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 4:33 下午 2022/1/23
  * @Modified By:
  */
trait AbstractRelationshipSerializer {
  def encodeRelationship(
      relationId: Long,
      fromId: Long,
      toId: Long,
      typeId: Int,
      props: Map[Int, Any]
  ): Array[Byte]
  def encodeRelationship(storedRelationship: StoredRelationship): Array[Byte]

  def decodeRelationshipWithProperties(
      bytes: Array[Byte]
  ): StoredRelationshipWithProperty
  def decodePropertiesFromFullRelationship(bytes: Array[Byte]): Map[Int, Any]

}
