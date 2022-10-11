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

package org.grapheco.tudb

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 17:50 2022/4/12
  * @Modified By:
  */
object TuDBInstanceContext extends ContextMap {

  def setDataPath(path: String): Unit = {
    // Todo: check whether the path is illegal.
    // for a new instance, the path should be empty
    // for an old instance, the path should include a description file.
    super.put("dataPath", path)
  }

  def getDataPath: String = super.get[String]("dataPath")

  def setPort(port: Int): Unit = super.put("bindPort", port)

  def getPort: Int = super.get[Int]("bindPort")

  def setIndexUri(indexUri: String) = super.put("indexUri", indexUri)

  def getIndexUri = super.get[String]("indexUri")

}
