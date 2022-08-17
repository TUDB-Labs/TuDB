package org.grapheco.lynx.operator.join

/**
  *@description: List Join Types.
  *
  *         Notice: Why no inner join? Because we can optimize innerJoin to expandFromNode.
  */
trait JoinType {}

case object JoinType {
  case object DEFAULT extends JoinType
}
