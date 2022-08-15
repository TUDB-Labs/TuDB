package org.grapheco.lynx.operator.join

/**
  *@description: List Join Types.
  *
  *         Notice: Why no inner join? Because we can optimize innerJoin to expandFromNode.
  */
trait JoinType {}

case object JoinType {
  /*
    Two table have no same variable.
    ```
    match (n:Person)
    match (m:City)
    return n,m
    ```
   */
  case object CartesianProduct extends JoinType

  /*
    Two table have properties reference
    ```
    match (n:Person)
    match (m:City) where m.name=n.city
    return n,m
    ```
   */
  case object ValueHashJoin extends JoinType
}
