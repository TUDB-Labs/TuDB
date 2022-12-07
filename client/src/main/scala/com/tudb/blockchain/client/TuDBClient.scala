package com.tudb.blockchain.client

import com.tudb.blockchain.network.{Query, TuQueryServiceGrpc}
import com.tudb.blockchain.network.TuQueryServiceGrpc.TuQueryServiceBlockingStub
import io.grpc.ManagedChannel
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import scala.collection.JavaConverters._
import java.util.concurrent.TimeUnit

/**
  *@description:
  */
class TuDBClient(host: String, port: Int) {
  private var channel: ManagedChannel = _
  private var blockingStub: TuQueryServiceBlockingStub = _
  try {
    channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build()
    blockingStub = TuQueryServiceGrpc.newBlockingStub(channel)
  } catch {
    case e: Exception => {
      if (channel != null) {
        channel.shutdown()
        while (!channel.awaitTermination(5, TimeUnit.SECONDS)) {}
      }
    }
  }
  def hopQuery(
      address: String,
      direction: String,
      lowerHop: Int,
      upperHop: Int,
      limit: Int
    ): Iterator[Query.QueryResponse] = {
    val request = Query.HopQueryRequest
      .newBuilder()
      .setAddress(address)
      .setDirection(direction)
      .setLowerHop(lowerHop)
      .setUpperHop(upperHop)
      .setLimit(limit)
      .build()

    blockingStub.hopQuery(request).asScala
  }

  def close(): Unit = {
    channel.shutdown()
  }
}
