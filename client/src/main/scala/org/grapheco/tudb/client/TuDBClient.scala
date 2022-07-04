package org.grapheco.tudb.client

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.grapheco.tudb.TuDBJsonTool.objectMapper
import org.grapheco.tudb.network.{Query, TuQueryServiceGrpc}

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.asScalaIteratorConverter

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 11:17 2022/4/2
  * @Modified By:
  */
class TuDBClient(host: String, port: Int) {

  val channel =
    NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
  val blockingStub = TuQueryServiceGrpc.newBlockingStub(channel)

  def query(stat: String): String = {
    val request: Query.QueryRequest =
      Query.QueryRequest.newBuilder().setStatement(stat).build()
    val response = {
      blockingStub.query(request).asScala
    }
    if (!response.hasNext) {
      "Empty"
    } else {
      val rs = response.next()
      if (rs.getMessage == "EMPTY") {
        rs.getMessage
      } else {
        rs.getResult
      }
    }
  }

  def getStatistics(): List[Any] = {
    val request: Query.QueryRequest =
      Query.QueryRequest.newBuilder().setStatement("").build()
    val response: Iterator[Query.QueryResponse] =
      blockingStub.queryStatistics(request).asScala
    val resultStr =
      response.next().getResult
    objectMapper.readValue(resultStr, classOf[List[Any]])
  }

  def shutdown(): Unit = {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

}
