package org.grapheco.lynx.operator.join

/**
  *@description: List Join Types.
  */
trait JoinType {}

case object JoinType {
  case object DEFAULT extends JoinType
}
