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

package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull}
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.opencypher.v9_0.expressions.Expression

/**
  *@description:
  */
case class PhysicalJoin(
    filterExpr: Seq[Expression],
    val isSingleMatch: Boolean,
    bigTableIndex: Int = 1
  )(a: PhysicalNode,
    b: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(a, b)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df1 = a.execute(ctx)
    val df2 = b.execute(ctx)

    val df = df1.join(df2, isSingleMatch, bigTableIndex)

    // TODO: eval function each time can only process one expression,
    //  so if there are many filterExpression, we will filter DataFrame several times. can speed up?
    if (filterExpr.nonEmpty) {
      val ec = ctx.expressionContext
      var filteredDataFrame: DataFrame = DataFrame.empty
      filterExpr.foreach(expr => {
        filteredDataFrame = df.filter { (record: Seq[LynxValue]) =>
          eval(expr)(ec.withVars(df.schema.map(_._1).zip(record).toMap)) match {
            case LynxBoolean(b) => b
            case LynxNull       => false
          }
        }(ec)
      })
      filteredDataFrame
    } else df
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalJoin =
    PhysicalJoin(filterExpr, isSingleMatch)(children0.head, children0(1), plannerContext)

  override val schema: Seq[(String, LynxType)] = (a.schema ++ b.schema).distinct
}
