package org.grapheco.tudb

import io.grpc.stub.StreamObserver
import org.grapheco.tudb.core.{Core, RelationshipServiceGrpc}
import org.grapheco.tudb.serializer.RelationshipSerializer
import org.grapheco.tudb.store.relationship.StoredRelationshipWithProperty

class RelationshipService(dbPath: String, indexUri: String)
  extends RelationshipServiceGrpc.RelationshipServiceImplBase {

  override def createRelationship(
      request: Core.RelationshipCreateRequest,
      responseObserver: StreamObserver[Core.RelationshipCreateResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully created relationship")
      .setExitCode(0)
      .build()
    // TODO: Create object and persist in DB and set status
    val resp: Core.RelationshipCreateResponse = Core.RelationshipCreateResponse
      .newBuilder()
      .setRelationship(request.getRelationship)
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def getRelationship(
      request: Core.RelationshipGetRequest,
      responseObserver: StreamObserver[Core.RelationshipGetResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully got relationship")
      .setExitCode(0)
      .build()
    val resp: Core.RelationshipGetResponse = Core.RelationshipGetResponse
      .newBuilder()
      // TODO: Get object from DB
      //      .setRelationship(request.getName)
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def deleteRelationship(
      request: Core.RelationshipDeleteRequest,
      responseObserver: StreamObserver[Core.RelationshipDeleteResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully deleted relationship")
      .setExitCode(0)
      .build()
    // TODO: Delete the object from DB
    val resp: Core.RelationshipDeleteResponse = Core.RelationshipDeleteResponse
      .newBuilder()
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }

  override def listRelationships(
      request: Core.RelationshipListRequest,
      responseObserver: StreamObserver[Core.RelationshipListResponse]
    ): Unit = {
    val status = Core.GenericResponseStatus
      .newBuilder()
      .setMessage("successfully listed relationships")
      .setExitCode(0)
      .build()
    // TODO: Delete the object from DB
    val resp: Core.RelationshipListResponse = Core.RelationshipListResponse
      .newBuilder()
      // TODO: Get objects from DB
      //      .setRelationships()
      .setStatus(status)
      .build()
    responseObserver.onNext(resp)
    responseObserver.onCompleted()
  }
}

object RelationshipService {
  def ConvertToGrpcRelationship(
      rawRelationship: StoredRelationshipWithProperty
    ): Core.Relationship = {
    val relationshipBuilder: Core.Relationship.Builder = Core.Relationship
      .newBuilder()
      .setRelationshipId(rawRelationship.id)

    rawRelationship.properties.foreach(kv => {
      val prop = Core.Property
        .newBuilder()
        .setInd(kv._1)
        .setValue(kv._2.toString)
        .build()
      relationshipBuilder.addProperties(prop)
    })
    relationshipBuilder.setStartNodeId(rawRelationship.from)
    relationshipBuilder.setEndNodeId(rawRelationship.to)
    relationshipBuilder.setRelationType(rawRelationship.typeId)
    relationshipBuilder.build()
  }

  def ConvertToStoredRelationship(
      relationship: Core.Relationship
    ): StoredRelationshipWithProperty = {
    var rawProps = Map[Int, String]()
    relationship.getPropertiesList.forEach(prop => {
      rawProps += (prop.getInd -> prop.getValue)
    })
    val fromNode: Long = relationship.getStartNodeId
    val toNode: Long = relationship.getEndNodeId
    val typeId: Int = relationship.getRelationType

    val relationshipInBytes: Array[Byte] =
      RelationshipSerializer.encodeRelationship(
        relationship.getRelationshipId,
        fromNode,
        toNode,
        typeId,
        rawProps
      )

    new StoredRelationshipWithProperty(
      relationship.getRelationshipId,
      fromNode,
      toNode,
      typeId,
      relationshipInBytes
    )
  }
}
