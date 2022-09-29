package org.grapheco.lynx.graph

import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}

/** An index consists of a label and several properties.
  * eg: Index("Person", Set("name"))
  * @param labelName The label
  * @param properties The properties, Non repeatable.
  */
case class Index(labelName: LynxNodeLabel, properties: Set[LynxPropertyKey])
