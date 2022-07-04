package org.grapheco.tudb

import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils
import org.grapheco.lynx.LynxException
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.tudb.TuDBJsonTool.AnyRefAddMethod
import org.grapheco.tudb.common.utils.LogUtil
import org.grapheco.tudb.exception.TuDBException
import org.grapheco.tudb.facade.GraphFacade
import org.grapheco.tudb.network.Query.QueryResponse
import org.grapheco.tudb.network.{Query, TuQueryServiceGrpc}
import org.opencypher.v9_0.util.CypherException
import org.slf4j.LoggerFactory

class TuDBQueryService(dbPath: String, indexUri: String)
  extends TuQueryServiceGrpc.TuQueryServiceImplBase {
  val LOGGER = LoggerFactory.getLogger(this.getClass)
  val db: GraphFacade = GraphDatabaseBuilder.newEmbeddedDatabase(dbPath, indexUri)
  var errorMessage: String = null

  override def query(
      request: Query.QueryRequest,
      responseObserver: StreamObserver[Query.QueryResponse]
    ): Unit = {
    try {
      val queryStat: String = request.getStatement
      val queryResult = db.cypher(queryStat)
      val resp: QueryResponse = QueryResponse
        .newBuilder()
        .setMessage("OK")
        .setMessage(queryResult.toJson())
        .build()
      responseObserver.onNext(resp)

    } catch {
      case e: LynxException =>
        errorMessage = lynxExceptionProcess(e)
      case e: CypherException =>
        errorMessage = cypherExceptionProcess(e)
      case e: TuDBException =>
        errorMessage = tuDbExceptionProcess(e)
      case e: Throwable =>
        errorMessage = systemExceptionProcess(e)
    } finally {
      if (!StringUtils.isEmpty(errorMessage)) {
        responseObserver.onNext(
          QueryResponse
            .newBuilder()
            .setMessage(errorMessage)
            .build()
        )
      }
      responseObserver.onCompleted()
    }

  }

  /** when lynx exception caught
    * @param e
    * @return response message
    */
  def lynxExceptionProcess(e: LynxException): String = {
    LogUtil.warn(LOGGER, "TuDB caught a lynx error: %s", e.getMessage)
    //TODO do something more
    e.getMessage
  }

  def cypherExceptionProcess(e: CypherException): String = {
    LogUtil.warn(LOGGER, "TuDB caught a cypher syntax error: %s", e.getMessage)
    e.getMessage
  }

  /** when tudb exception caught
    * @param e
    * @return response message
    */
  def tuDbExceptionProcess(e: TuDBException): String = {
    LogUtil.error(LOGGER, e, "TuDB caught a business error:%s", e.getMessage)
    // TODO do something more
    "TuDB error,code is " + e.getCode
  }

  /** when other exception caught
    * @param e
    * @return response message
    */
  def systemExceptionProcess(e: Throwable): String = {
    LogUtil.error(LOGGER, e, "System error:", e.getMessage)
    // TODO do something more
    "System Error!"
  }

  override def queryStatistics(
      request: Query.QueryRequest,
      responseObserver: StreamObserver[QueryResponse]
    ): Unit = {
    val nodeCountByLabel: LynxMap = db.statistics.getNodeCountByLabel()
    val relationshipCountByType: LynxMap =
      db.statistics.getRelationshipCountByType()
    val statisticsList: LynxList = LynxList(
      List(nodeCountByLabel, relationshipCountByType)
    )
    val statisticsJson = statisticsList.toJson()
    val resp: QueryResponse = QueryResponse
      .newBuilder()
      .setMessage("OK")
      .setMessage(statisticsJson)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}
