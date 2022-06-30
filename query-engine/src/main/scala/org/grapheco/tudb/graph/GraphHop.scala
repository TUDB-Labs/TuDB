package org.grapheco.tudb.graph

/** @author:John117
  * @createDate:2022/6/28
  * @description:
  */
/** @param paths this hop's all paths
  *
  *    example:
  *
  *              A
  *          /   |   \
  *        B     C    D
  *       /      |
  *      E       F
  *     |
  *     G
  *
  *     each pattern like [A->B], [A->B->E], [A->B->E->G] called as a path
  *     hop is a collection of paths.
  *
  *     A->B, A->C, A->D then GraphHop( Seq(A->B, A->C, A->D ) ) called one hop
  *     A->B-E, A->C->F then GraphHop(Seq( A->B->E, A->C->F  ) ) called two hop
  *     A->B->E->G then GraphHop( Seq( A->B->E->G )) called three hop
  */
case class GraphHop(paths: Seq[GraphPath]) {}
