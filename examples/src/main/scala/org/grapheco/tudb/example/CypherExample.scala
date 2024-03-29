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

package org.grapheco.tudb.example

import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.tudb.{TuDBServer, TuDBServerContext}
import org.grapheco.tudb.client.TuDBClient
import org.grapheco.tudb.test.TestUtils

import java.io.File

/** @author:John117
  * @createDate:2022/5/30
  * @description:
  */
object CypherExample {
  val port = 7600
  var server: TuDBServer = _
  var client: TuDBClient = _
  def main(args: Array[String]): Unit = {
    val dbPath: String = s"${TestUtils.getModuleRootPath}/testSpace/testBase"
    server = startServer(dbPath, port)
    startClient()

    createNode()
    queryNode()
    createRelation()
    queryRelation()

    stopClient()
    shutdownServer()
  }

  /**
    *
    * @param dbPath
    * @param port
    * @param indexUrl
    *  index engine url
    # has four implement
    # tudb://index?type=memory    tudb://index?type=elasticsearch&ip=xx&port=xx    tudb://index?type=rocksdb
    # tudb://index?type=dummy
    # memory  use hashmap storage index data
    # elasticsearch   use  elasticsearch storage index data,ip and port is es service address
    # rocksdb use rocksdb storage index data
    # dummy is empty implement ,  use this engine where no  index is used
    * @return
    */
  def startServer(
      dbPath: String,
      port: Int,
      indexUrl: String = "tudb://index?type=dummy"
    ): TuDBServer = {
    FileUtils.deleteDirectory(new File(dbPath))
    val serverContext = new TuDBServerContext()
    serverContext.setPort(port)
    serverContext.setDataPath(dbPath)
    serverContext.setIndexUri(indexUrl)
    val server = new TuDBServer(serverContext)
    new Thread(new Runnable {
      override def run(): Unit = server.start()
    }).start()
    server
  }
  def shutdownServer() = server.shutdown()

  def startClient(): Unit = {
    client = new TuDBClient("127.0.0.1", port)
  }
  def stopClient() = client.shutdown()

  def createNode(): Unit = {
    client.query("create (n:DataBase{name:'TuDB'})")
    client.query("create (n: Company{name:'TUDB'})")
  }
  def queryNode(): Unit = {
    val res = client.query("match (n) return n")
    println()
    println("Query Node result: ", res)
    println()
  }

  def createRelation(): Unit = {
    client.query("""
        |match (n:DataBase{name:'TuDB'})
        |match (c: Company{name:'TUDB'})
        |create (n)-[r: belongTo{year: 2022}]->(c)
        |""".stripMargin)
  }
  def queryRelation(): Unit = {
    val res = client.query("match (n)-[r: belongTo]->(m) return n,r,m")
    println()
    println("Query Relation Result: ", res)
  }
}
