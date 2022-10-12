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

package org.grapheco.lynx.graph

/** Manage index creation and deletion.
  */
trait IndexManager {

  /** Create Index.
    * @param index The index to create
    */
  def createIndex(index: Index): Unit

  /** Drop Index.
    * @param index The index to drop
    */
  def dropIndex(index: Index): Unit

  /** Get all indexes.
    * @return all indexes
    */
  def indexes: Array[Index]
}
