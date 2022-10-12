// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.lynx.execution

import org.grapheco.lynx.graph.GraphModel
import org.grapheco.lynx.physical.filters.NodeFilter
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.grapheco.lynx.{CypherRunnerContext, ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch, TreeNode}
import org.opencypher.v9_0.expressions.{Expression, LabelName, LogicalVariable, NodePattern}
import org.opencypher.v9_0.util.symbols.CTNode

/**
  *@author:John117
  *@createDate:2022/7/29
  *@description:
  */
case class NodeScanOperator(
    pattern: NodePattern,
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq.empty

  var schema: Seq[(String, LynxType)] = Seq.empty
  var dataSource: Iterator[RowBatch] = Iterator.empty

  // prepare data
  override def openImpl(): Unit = {
    val NodePattern(
      Some(nodeVariable: LogicalVariable),
      labels: Seq[LabelName],
      properties: Option[Expression],
      baseNode: Option[LogicalVariable]
    ) = pattern

    schema = Seq(nodeVariable.name -> CTNode)

    val nodeLabels = {
      if (labels.nonEmpty) labels.map(_.name).map(LynxNodeLabel)
      else Seq.empty
    }
    dataSource = graphModel
      .nodes(
        NodeFilter(
          nodeLabels,
          properties
            .map(prop =>
              expressionEvaluator
                .eval(prop)(expressionContext)
                .asInstanceOf[LynxMap]
                .value
                .map(kv => (LynxPropertyKey(kv._1), kv._2))
            )
            .getOrElse(Map.empty)
        )
      )
      .grouped(numRowsPerBatch)
      .map(node => Seq(node))
      .map(f => RowBatch(f))
  }

  override def getNextImpl(): RowBatch = {
    if (dataSource.nonEmpty) dataSource.next()
    else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = schema

}
