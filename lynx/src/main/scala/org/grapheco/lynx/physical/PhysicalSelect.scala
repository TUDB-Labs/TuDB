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
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}

/**
  *@description:
  */
case class PhysicalSelect(
    columns: Seq[(String, Option[String])]
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalSelect =
    PhysicalSelect(columns)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] =
    columns.map(x => x._2.getOrElse(x._1)).map(x => x -> in.schema.find(_._1 == x).get._2)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.select(columns)
  }
}
