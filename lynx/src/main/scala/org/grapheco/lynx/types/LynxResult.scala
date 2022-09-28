package org.grapheco.lynx.types

/**
  *@description:
  */
trait LynxResult {
  def show(limit: Int = 20): Unit

  def cache(): LynxResult

  def columns(): Seq[String]

  def records(): Iterator[Map[String, LynxValue]]
}
