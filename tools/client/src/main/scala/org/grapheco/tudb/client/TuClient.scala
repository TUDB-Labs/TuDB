package org.grapheco.tudb.client

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf
import org.grapheco.lynx.lynxrpc.{LynxByteBufFactory, LynxValueDeserializer}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
import org.grapheco.tudb.network.{Query, TuQueryServiceGrpc}

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.asScalaIteratorConverter

/** @Author: Airzihao
  * @Description:
  * @Date: Created at 11:17 2022/4/2
  * @Modified By:
  */
class TuClient(host: String, port: Int) {

  val channel =
    NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
  val blockingStub = TuQueryServiceGrpc.newBlockingStub(channel)

  def query(stat: String): Iterator[Map[String, LynxValue]] = {
    val request: Query.QueryRequest =
      Query.QueryRequest.newBuilder().setStatement(stat).build()
    val response: Iterator[Query.QueryResponse] =
      blockingStub.query(request).asScala
    val byteBuf: ByteBuf = LynxByteBufFactory.getByteBuf
    val lynxValueDeserializer: LynxValueDeserializer = new LynxValueDeserializer
    if (!response.hasNext) {
      Iterator()
    } else {
      response.map(resp =>
        lynxValueDeserializer
          .decodeLynxValue(
            byteBuf.writeBytes(resp.getResultInBytes.toByteArray)
          )
          .asInstanceOf[LynxMap]
          .value
      )
    }
  }

  def getStatistics(): List[LynxMap] = {
    val request: Query.QueryRequest =
      Query.QueryRequest.newBuilder().setStatement("").build()
    val response: Iterator[Query.QueryResponse] =
      blockingStub.queryStatistics(request).asScala
    val byteBuf: ByteBuf = LynxByteBufFactory.getByteBuf
    val lynxValueDeserializer: LynxValueDeserializer = new LynxValueDeserializer
    val resultInBytes: Array[Byte] =
      response.next().getResultInBytes.toByteArray
    lynxValueDeserializer
      .decodeLynxValue(byteBuf.writeBytes(resultInBytes))
      .value
      .asInstanceOf[List[LynxMap]]
  }

  def shutdown(): Unit = {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

}
