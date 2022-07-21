package org.grapheco.lynx.procedure.functions

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.procedure.exceptions.LynxProcedureException
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxFloat, LynxInteger, LynxNull, LynxNumber, LynxString}

/** @ClassName NumericFunctions
  * @Description These functions all operate on numerical expressions only,
  * and will return an error if used on any other values.
  * @Author huchuan
  * @Date 2022/4/20
  * @Version 0.1
  */
class NumericFunctions {
  @LynxProcedure(name = "abs")
  def abs(args: Seq[LynxValue]): LynxNumber = {
    args.head match {
      case i: LynxInteger => LynxInteger(math.abs(i.value))
      case d: LynxFloat   => LynxFloat(math.abs(d.value))
      case n @ LynxNull   => null
    }
  }

  @LynxProcedure(name = "ceil")
  def ceil(args: Seq[LynxNumber]): LynxNumber = {
    LynxFloat(math.ceil(args.head.number.doubleValue()))
  }

  @LynxProcedure(name = "floor")
  def floor(args: Seq[LynxNumber]): LynxNumber = {
    LynxFloat(math.floor(args.head.number.doubleValue()))
  }

  @LynxProcedure(name = "rand")
  def rand(args: Seq[LynxValue]): LynxNumber = {
    LynxFloat(math.random())
  }

  @LynxProcedure(name = "round")
  def round(args: Seq[LynxValue]): LynxValue = {
    args.size match {
      case 1 => LynxInteger(math.round(args.head.asInstanceOf[LynxNumber].number.doubleValue()))
      case 2 => { // precision
        val x = args.head.asInstanceOf[LynxNumber]
        val precision = args.last.asInstanceOf[LynxInteger]
        val base = math.pow(10, precision.value)
        LynxFloat(math.round(base * x.number.doubleValue()).toDouble / base)
      }
      case 3 => { // mode
        val x = args.head.asInstanceOf[LynxNumber]
        val precision = args(1).asInstanceOf[LynxInteger]
        val mode = args(2).asInstanceOf[LynxString].value
        val base = math.pow(10, precision.value)
        var result = base * x.number.doubleValue()
        if (mode == "CEILING") {
          result = math.ceil(result) / base
        } else if (mode == "FLOOR") {
          result = math.floor(result) / base
        } else {
          throw LynxProcedureException(
            s"round() can only support the following mode: CEILING or FLOOR. Got ${mode}"
          )
        }
        LynxFloat(result)
      }
    }
  }

  @LynxProcedure(name = "sign")
  def sign(args: Seq[LynxNumber]): LynxNumber = {
    LynxFloat(math.signum(args.head.number.doubleValue()))
  }
}
