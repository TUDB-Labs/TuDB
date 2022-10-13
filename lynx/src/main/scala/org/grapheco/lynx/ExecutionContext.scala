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

package org.grapheco.lynx

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.opencypher.v9_0.ast.Statement

/**
  *@description:
  */
case class ExecutionContext(
    physicalPlannerContext: PhysicalPlannerContext,
    statement: Statement,
    queryParameters: Map[String, Any]) {
  val expressionContext = ExpressionContext(
    this,
    queryParameters.map(x => x._1 -> physicalPlannerContext.runnerContext.typeSystem.wrap(x._2))
  )
}
