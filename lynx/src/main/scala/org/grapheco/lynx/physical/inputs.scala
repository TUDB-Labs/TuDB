package org.grapheco.lynx.physical

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxId, LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}

/**
  *@description:
  */
case class NodeInput(labels: Seq[LynxNodeLabel], props: Seq[(LynxPropertyKey, LynxValue)]) {}

case class RelationshipInput(
    types: Seq[LynxRelationshipType],
    props: Seq[(LynxPropertyKey, LynxValue)],
    startNodeRef: NodeInputRef,
    endNodeRef: NodeInputRef) {}

sealed trait NodeInputRef

case class StoredNodeInputRef(id: LynxId) extends NodeInputRef

case class ContextualNodeInputRef(varname: String) extends NodeInputRef
