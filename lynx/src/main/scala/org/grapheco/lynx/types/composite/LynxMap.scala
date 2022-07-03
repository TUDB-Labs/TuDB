package org.grapheco.lynx.types.composite

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.util.symbols.{CTMap, CypherType}

/**
  * @ClassName LynxMap
  * @Description TODO
  * @Author huchuan
  * @Date 2022/4/1
  * @Version 0.1
  */
case class LynxMap(v: Map[String, LynxValue]) extends LynxCompositeValue {
  override def value: Map[String, LynxValue] = v

  override def lynxType: CypherType = CTMap
}
