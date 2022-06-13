package org.grapheco.tudb

import com.google.protobuf.ByteString
import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.{NettyServerBuilder => SNettyServerBuilder}
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf
import io.grpc.stub.StreamObserver
import org.grapheco.lynx.lynxrpc.{LynxByteBufFactory, LynxValueSerializer}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.network.Query.QueryResponse
import org.grapheco.tudb.network.{Query, TuQueryServiceGrpc}


/** @Author: Airzihao
  * @Description:
  * @Date: Created at 15:35 2022/4/1
  * @Modified By:
  */
class TuDBServer(bindPort: Int, dbPath: String) {

  private val _port: Int = bindPort
  private val _server: Server = SNettyServerBuilder
    .forPort(_port)
    .addService(new QueryServiceImpl(dbPath))
    .build()

  def start(): Unit = {
    _server.start()
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        _server.shutdown()
      }
    })
    _server.awaitTermination()
  }

  def shutdown(): Unit = {
    _server.shutdown()
  }

}

class QueryServiceImpl(dbPath: String)
    extends TuQueryServiceGrpc.TuQueryServiceImplBase {
  val db: GraphFacade = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath)

  override def query(
      request: Query.QueryRequest,
      responseObserver: StreamObserver[Query.QueryResponse]
  ): Unit = {
    val lynxValueSerializer: LynxValueSerializer = new LynxValueSerializer
    val byteBuf: ByteBuf = LynxByteBufFactory.getByteBuf
    val queryStat: String = request.getStatement
    val queryResultIter = db.cypher(queryStat).records()

    if (!queryResultIter.hasNext) {
      responseObserver.onCompleted()
    } else {
      while (queryResultIter.hasNext) {
        val rowInBytes: Array[Byte] =
          LynxByteBufFactory.exportBuf(
            lynxValueSerializer.encodeLynxValue(
              byteBuf,
              LynxMap(queryResultIter.next())
            )
          )
        val resp: QueryResponse = QueryResponse
          .newBuilder()
          .setResultInBytes(ByteString.copyFrom(rowInBytes))
          .build()
        responseObserver.onNext(resp)
      }
      responseObserver.onCompleted()
    }
  }

  override def queryStatistics(
      request: Query.QueryRequest,
      responseObserver: StreamObserver[QueryResponse]
  ): Unit = {
    val lynxValueSerializer: LynxValueSerializer = new LynxValueSerializer
    val byteBuf: ByteBuf = LynxByteBufFactory.getByteBuf
    val nodeCountByLabel: LynxMap = db.statistics.getNodeCountByLabel()
    val relationshipCountByType: LynxMap =
      db.statistics.getRelationshipCountByType()
    val statisticsList: LynxList = LynxList(
      List(nodeCountByLabel, relationshipCountByType)
    )
    val bytes: Array[Byte] = LynxByteBufFactory.exportBuf(
      lynxValueSerializer.encodeLynxValue(byteBuf, statisticsList)
    )
    val resp: QueryResponse = QueryResponse
      .newBuilder()
      .setResultInBytes(ByteString.copyFrom(bytes))
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}
