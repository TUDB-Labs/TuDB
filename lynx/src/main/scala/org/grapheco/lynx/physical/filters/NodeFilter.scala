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

package org.grapheco.lynx.physical.filters

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural.{LynxNode, LynxNodeLabel, LynxPropertyKey}

/**
  *@description:
  */
/** labels note: the node with both LABEL1 and LABEL2 labels.
  * @param labels label names
  * @param properties filter property names
  */
case class NodeFilter(labels: Seq[LynxNodeLabel], properties: Map[LynxPropertyKey, LynxValue]) {
  def matches(node: LynxNode): Boolean =
    labels.forall(node.labels.contains) &&
      properties.forall {
        case (propertyName, value) =>
          node.property(propertyName).exists(value.equals)
      }
}
