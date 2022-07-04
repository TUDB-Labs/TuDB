package org.grapheco.lynx.procedure.functions

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNumber

/** @ClassName TrigonometricFunctions
 * @Description These functions all operate on numerical expressions only,
 * and will return an error if used on any other values.
 * All trigonometric functions operate on radians, unless otherwise specified.
 * @Author huchuan
 * @Date 2022/4/20
 * @Version 0.1
 */
class TrigonometricFunctions {
  @LynxProcedure(name = "acos")
  def acos(args: Seq[LynxNumber]): Double = {
    math.acos(args.head.number.doubleValue())
  }

  @LynxProcedure(name = "asin")
  def asin(args: Seq[LynxNumber]): Double = {
    math.asin(args.head.number.doubleValue())
  }

  @LynxProcedure(name = "atan")
  def atan(args: Seq[LynxNumber]): Double = {
    math.atan(args.head.number.doubleValue())
  }

  @LynxProcedure(name = "atan2")
  def atan2(args: Seq[LynxNumber]): Double = {
    val x = args.head
    val y = args.last
    math.atan2(x.number.doubleValue(), y.number.doubleValue())
  }

  @LynxProcedure(name = "cos")
  def cos(args: Seq[LynxNumber]): Double = {
    math.cos(args.head.number.doubleValue())
  }

  @LynxProcedure(name = "cot")
  def cot(args: Seq[LynxNumber]): Double = {
    1.0 / math.tan(args.head.number.doubleValue())
  }

  @LynxProcedure(name = "degrees")
  def degrees(args: Seq[LynxNumber]): Double = {
    math.toDegrees(args.head.number.doubleValue())
  }

  //  @LynxProcedure(name = "haversin")
  //  def haversin(x: LynxNumber): Double = {
  //    (1.0d - math.cos(x.number.doubleValue())) / 2
  //  }

  @LynxProcedure(name = "pi")
  def pi(args: Seq[LynxValue]): Double = {
    Math.PI
  }

  @LynxProcedure(name = "radians")
  def radians(args: Seq[LynxNumber]): Double = {
    math.toRadians(args.head.number.doubleValue())
  }

  @LynxProcedure(name = "sin")
  def sin(args: Seq[LynxNumber]): Double = {
    math.sin(args.head.number.doubleValue())
  }

  @LynxProcedure(name = "tan")
  def tan(args: Seq[LynxNumber]): Double = {
    math.tan(args.head.number.doubleValue())
  }
}