package org.grapheco.tudb

import com.google.protobuf.ByteString
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf
import io.grpc.stub.StreamObserver
import org.grapheco.lynx.lynxrpc.{LynxByteBufFactory, LynxValueSerializer}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.network.Query.QueryResponse
import org.grapheco.tudb.network.{Query, TuQueryServiceGrpc}

class TuDBQueryService(dbPath: String,indexUri:String)
  extends TuQueryServiceGrpc.TuQueryServiceImplBase {
  val db: GraphFacade = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath,indexUri)

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
