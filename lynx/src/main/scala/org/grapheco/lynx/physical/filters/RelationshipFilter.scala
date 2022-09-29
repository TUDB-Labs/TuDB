package org.grapheco.lynx.physical.filters

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxPropertyKey, LynxRelationship, LynxRelationshipType}

/**
  *@description:
  */
/** types note: the relationship of type TYPE1 or of type TYPE2.
  * @param types type names
  * @param properties filter property names
  */
case class RelationshipFilter(
    types: Seq[LynxRelationshipType],
    properties: Map[LynxPropertyKey, LynxValue]) {
  def matches(relationship: LynxRelationship): Boolean =
    ((types, relationship.relationType) match {
      case (Seq(), _)          => true
      case (_, None)           => false
      case (_, Some(typeName)) => types.contains(typeName)
    }) && properties.forall {
      case (propertyName, value) =>
        relationship.property(propertyName).exists(value.equals)
    }
}
