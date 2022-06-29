/** Copyright (c) 2022 PandaDB * */
package org.grapheco.tudb.test.server

import io.grpc.stub.StreamObserver
import org.grapheco.tudb.{TuDBQueryService}
import org.grapheco.tudb.network.Query
import org.junit._
import org.junit.runners.MethodSorters

/** @Author: huanglin
 * @Description:
 * @Date: Created at 2022-6-29
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class ServerTest {

  @Test
  def testQueryReturnJson(): Unit = {
    val server = new TuDBQueryService("ldbc0.003.db","tudb://index?type=dummy")
    val responseObserver: StreamObserver[Query.QueryResponse] = new StreamObserver[Query.QueryResponse] {
      override def onNext(value: Query.QueryResponse): Unit = {
        println(value.getResultInBytes.toStringUtf8)
      }
      override def onError(t: Throwable): Unit = {
        println(t.getMessage)
      }
      override def onCompleted(): Unit = {
        println("onCompleted")
      }
    }
    server.query(Query.QueryRequest.newBuilder().setStatement("match (n)-[r*1..3]->(m) return r").build(),responseObserver)
  }


  @Before
  def cleanUp(): Unit = {

  }

  @After
  def close(): Unit = {

  }


}
