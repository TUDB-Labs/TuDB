
# TuDB

## What is TuDB?

TuDB is a cloud native graph database. TuDB aims to support Hybrid Transactional and Analytical Processing (HTAP)
workloads. It is compatible with Cypher and features horizontal scalability, strong consistency, and high availability.
TuDB aims to provide a unified graph process interface for constructing and managing graphs on different key-value
storage engines, such as RocksDB, HBase, and TiKV.

## Quick Start

### Installation

You can install TuDB by following these steps:

1. Download or clone the repo locally.
2. [Install Maven](https://maven.apache.org/install.html).
3. Run the following to build TuDB binary:

```bash
mvn clean compile install
```

### Examples

Below is an example to start the server and client locally and interact with the database
via Cypher queries. For more examples, please check out the [examples folder](../examples).

```scala
import org.apache.commons.io.FileUtils
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.tudb.{TuDBServer, TuDBServerContext}
import org.grapheco.tudb.client.TuDBClient
import org.grapheco.tudb.test.TestUtils
import java.io.File

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
```


## Development

You can run all the tests via the following:

```bash
mvn clean install test
```

You can start a database instance locally via `./bin/start`
or start an interactive shell via `./bin/cypher`.

For more details on how to contribute, please check out our [contributing guide](CONTRIBUTING.md).
