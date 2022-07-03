package org.grapheco.lynx.types.property

import org.grapheco.lynx.types.LynxValue
import org.opencypher.v9_0.util.symbols.{CTFloat, FloatType}

/**
  * @ClassName LynxDouble
  * @Description TODO
  * @Author huchuan
  * @Date 2022/4/1
  * @Version 0.1
  */
case class LynxFloat(v: Double) extends LynxNumber {
  def value: Double = v

  def number: Number = v

  def lynxType: FloatType = CTFloat

  def +(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxFloat(v + v2)
      case LynxFloat(v2)   => LynxFloat(v + v2)
    }
  }

  def -(that: LynxNumber): LynxNumber = {
    that match {
      case LynxInteger(v2) => LynxFloat(v - v2)
      case LynxFloat(v2)   => LynxFloat(v - v2)
    }
  }

  override def >(lynxValue: LynxValue): Boolean =
    this.value > lynxValue.asInstanceOf[LynxFloat].value

  override def >=(lynxValue: LynxValue): Boolean =
    this.value >= lynxValue.asInstanceOf[LynxFloat].value

  override def <(lynxValue: LynxValue): Boolean =
    this.value < lynxValue.asInstanceOf[LynxFloat].value

  override def <=(lynxValue: LynxValue): Boolean =
    this.value <= lynxValue.asInstanceOf[LynxFloat].value
}
