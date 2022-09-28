package org.grapheco.lynx.logical

import org.opencypher.v9_0.expressions.{LabelName, PropertyKeyName}

/**
  *@description:
  */
case class LogicalCreateIndex(labelName: LabelName, properties: List[PropertyKeyName])
  extends LogicalNode
