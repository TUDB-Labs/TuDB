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

import org.grapheco.lynx.logical.LogicalNode
import org.grapheco.lynx.logical.plan.LogicalPlannerContext

/**
  *@description: pipelines a set of LogicalNodes
  */
case class PipedTranslators(items: Seq[LogicalNodeTranslator]) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    items
      .foldLeft[Option[LogicalNode]](in) { (in, item) =>
        Some(item.translate(in)(plannerContext))
      }
      .get
  }
}