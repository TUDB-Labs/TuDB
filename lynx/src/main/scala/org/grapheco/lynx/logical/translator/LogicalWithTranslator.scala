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

package org.grapheco.lynx.logical.translator

import org.grapheco.lynx.logical.plan.LogicalPlannerContext
import org.grapheco.lynx.logical.{LogicalCreateUnit, LogicalNode}
import org.opencypher.v9_0.ast.{Limit, ReturnItems, Skip, Where, With}

/**
  *@description:
  */
case class LogicalWithTranslator(w: With) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    (w, in) match {
      case (
          With(
            distinct,
            ReturnItems(includeExisting, items),
            orderBy,
            skip,
            limit: Option[Limit],
            where
          ),
          None
          ) =>
        LogicalCreateUnit(items)

      case (
          With(
            distinct,
            ri: ReturnItems,
            orderBy,
            skip: Option[Skip],
            limit: Option[Limit],
            where: Option[Where]
          ),
          Some(sin)
          ) =>
        PipedTranslators(
          Seq(
            LogicalProjectTranslator(ri),
            LogicalWhereTranslator(where),
            LogicalSkipTranslator(skip),
            LogicalOrderByTranslator(orderBy),
            LogicalLimitTranslator(limit),
            LogicalSelectTranslator(ri),
            LogicalDistinctTranslator(distinct)
          )
        ).translate(in)
    }
  }
}
