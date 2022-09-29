package org.grapheco.lynx.logical

import org.opencypher.v9_0.ast.ReturnItem

/**
  *@description:
  */
case class LogicalCreateUnit(items: Seq[ReturnItem]) extends LogicalNode {}
