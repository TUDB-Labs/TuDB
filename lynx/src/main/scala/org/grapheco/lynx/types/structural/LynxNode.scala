package org.grapheco.lynx.types.structural

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.opencypher.v9_0.util.symbols.{CTNode, NodeType}

trait LynxNode extends LynxValue with HasProperty {
  val id: LynxId

  def value: LynxNode = this

  def labels: Seq[LynxNodeLabel]

  def lynxType: NodeType = CTNode
}
