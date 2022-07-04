package org.grapheco.lynx.procedure.functions

import org.grapheco.lynx.func.LynxProcedure
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxInteger, LynxString}

/** @ClassName StringFunctions
  * @Description These functions are used to manipulate strings or to create a string representation of another value
  * @Author Hu Chuan
  * @Date 2022/4/20
  * @Version 0.1
  */
class StringFunctions {
  @LynxProcedure(name = "left")
  def left(args: Seq[LynxValue]): String = {
    val x = args.head.asInstanceOf[LynxString]
    val endIndex = args.last.asInstanceOf[LynxInteger]
    val str = x.value
    if (endIndex.value.toInt < str.length) str.substring(0, endIndex.value.toInt)
    else str
  }

  @LynxProcedure(name = "lTrim")
  def lTrim(args: Seq[LynxString]): String = {
    val str = args.head.value
    if (str == "" || str == null) str
    else args.head.value.replaceAll(s"^[  ]+", "")
  }

  @LynxProcedure(name = "replace")
  def replace(args: Seq[LynxString]): String = {
    val x = args.head
    val search = args(1)
    val replace = args.last
    val str = x.value
    if (str == "" || str == null) str
    else str.replaceAll(search.value, replace.value)
  }

  @LynxProcedure(name = "reverse")
  def reverse(args: Seq[LynxString]): String = {
    val str = args.head.value
    if (str == "" || str == null) str
    else str.reverse
  }

  @LynxProcedure(name = "right")
  def right(args: Seq[LynxValue]): String = {
    val x = args.head.asInstanceOf[LynxString]
    val endIndex = args.last.asInstanceOf[LynxInteger]
    val str = x.value
    if (endIndex.value.toInt < str.length) str.substring(endIndex.value.toInt - 1)
    else str
  }

  @LynxProcedure(name = "rTrim")
  def rTrim(args: Seq[LynxString]): String = {
    val str = args.head.value
    if (str == "" || str == null) str
    else args.head.value.replaceAll(s"[　 ]+$$", "")
  }

  @LynxProcedure(name = "split")
  def split(args: Seq[LynxString]): Array[String] = {
    val x = args.head
    val regex = args.last
    val str = x.value
    if (str == "" || str == null) Array(str)
    else str.split(regex.value)
  }

  @LynxProcedure(name = "substring")
  def substring(args: Seq[LynxValue]): String = {
    args.size match {
      case 2 => {
        val x = args.head.asInstanceOf[LynxString]
        val left = args.last.asInstanceOf[LynxInteger]
        val str = x.value
        if (str == "" || str == null) str
        else str.substring(left.value.toInt)
      }
      case 3 => {
        val x = args.head.asInstanceOf[LynxString]
        val left = args(1).asInstanceOf[LynxInteger]
        val length = args.last.asInstanceOf[LynxInteger]
        val str = x.value
        if (str == "" || str == null) str
        else {
          if (left.value.toInt + length.value.toInt < str.length)
            str.substring(left.value.toInt, left.value.toInt + length.value.toInt)
          else str.substring(left.value.toInt)
        }
      }
    }
  }

  @LynxProcedure(name = "toLower")
  def toLower(args: Seq[LynxString]): String = {
    args.head.value.toLowerCase
  }

  @LynxProcedure(name = "toString")
  def toString(args: Seq[LynxValue]): String = args.head match {
    //    case dr: LynxDuration => dr.toString
    case _ => args.head.value.toString
  }

  @LynxProcedure(name = "toUpper")
  def toUpper(args: Seq[LynxString]): String = {
    args.head.value.toUpperCase
  }

  @LynxProcedure(name = "trim")
  def trim(args: Seq[LynxString]): String = {
    val str = args.head.value
    if (str == "" || str == null) str
    else str.trim
  }
  @LynxProcedure(name = "ltrim")
  def ltrim(args: Seq[LynxString]): String = {
    val str = args.head.value
    if (str == "" || str == null) str
    else {
      val index = str.indexWhere(p => p != ' ')
      str.slice(index, str.length)
    }
  }
  @LynxProcedure(name = "rtrim")
  def rtrim(args: Seq[LynxString]): String = {
    val str = args.head.value
    if (str == "" || str == null) str
    else {
      val index = str.indexWhere(p => p != ' ')
      str.slice(0, index + str.trim.length)
    }
  }
}
