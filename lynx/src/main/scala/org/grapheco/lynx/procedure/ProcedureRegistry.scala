package org.grapheco.lynx.procedure

trait ProcedureRegistry {
  def getProcedure(prefix: List[String], name: String): Option[CallableProcedure]
}
