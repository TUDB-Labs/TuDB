package org.grapheco.tudb.facade

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx._
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.grapheco.tudb.facade.helper.{ExpandFromNodeHelper, PathHelper}
import org.grapheco.tudb.store.meta.TuDBStatistics
import org.grapheco.tudb.store.node._
import org.grapheco.tudb.store.relationship._
import org.opencypher.v9_0.expressions.SemanticDirection
import org.grapheco.tudb.TuDBStoreContext

import scala.language.implicitConversions

/** @ClassName GraphFacade
  * @Description Needs further impl.
  * @Author huchuan
  * @Date 2022/3/24
  * @Version 0.1
  */
class GraphFacade(tuDBStatistics: TuDBStatistics, onClose: => Unit)
  extends LazyLogging
  with GraphModel {

  /** before delete nodes we should check is the nodes have relationships.
    * if nodes have relationships but not force to delete, we should throw exception,
    * otherwise we should delete relationships first then delete nodes.
    *
    * @param nodesIDs The ids of nodes to deleted
    * @param forced   When some nodes have relationships,
    *                 if it is true, delete any related relationships,
    *                 otherwise throw an exception
    */
  override def deleteNodesSafely(nodesIDs: Iterator[LynxId], forced: Boolean): Unit = {
    val ids = nodesIDs.toSet
    val affectedRelationships = ids
      .map(nid => {
        val id = nid.toLynxInteger.value
        findOutRelations(id) ++ findInRelations(id)
      })
      .foldLeft(Iterator[StoredRelationship]())((a, b) => a ++ b)
    // TODO: BATCH DELETE
    if (affectedRelationships.nonEmpty) {
      if (forced)
        deleteRelations(affectedRelationships.map(r => LynxRelationshipId(r.id)))
      else
        throw ConstrainViolatedException(
          s"deleting nodes with relationships, if force to delete, please use DETACH DELETE."
        )
    }
    deleteNodes(ids.toSeq)
  }

  /**
    * Expand function for cypher like (a)-[r1]-(b)-[r2]-(c)
    * when get the left relationship (a)-[r1]-(b), we need to expand relation from (b)
    * the reason why we don't use direction is because we only expand from (b)
    * so we only need findOutRelation.
    *
    * @param nodeId The id of this node
    * @param relationshipFilter conditions for relationships
    * @param endNodeFilter Filter condition of ending node
    * @param direction The direction of expansion, INCOMING, OUTGOING or BOTH
    *  @return Triples after expansion and filter
    */
  override def expand(
      node: LynxNode,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection,
      lowerLimit: Int,
      upperLimit: Int
    ): Iterator[Seq[PathTriple]] = {
    val helper = new ExpandFromNodeHelper(this)

    (lowerLimit, upperLimit) match {
      case (1, 1) =>
        helper
          .getExpandPathWithoutLength(node, relationshipFilter, endNodeFilter, direction)
          .map(Seq(_))
      case _ =>
        helper.getExpandPathsWithLength(
          node,
          relationshipFilter,
          endNodeFilter,
          direction,
          lowerLimit,
          upperLimit
        )
    }
  }

  /*
    The default implement of paths in lynx will scan all the relationships then use filter to get the data we want,
    and it cannot deal with the situation of relationship with variable length,
    the cypher like (a)-[r:TYPE*1..3]->(b).

    In TuDB, we have the relationship-index (findInRelationship and findOutRelationship),
    and we can speed up the search.
    So we need to override the paths, and impl the relationship with variable length.
   */
  override def paths(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection,
      upperLimit: Option[Int],
      lowerLimit: Option[Int]
    ): Iterator[Seq[PathTriple]] = {
    val pathHelper = new PathHelper(this)
    if (upperLimit.isDefined || lowerLimit.isDefined) {
      val lower = lowerLimit.getOrElse(1)
      val upper = upperLimit.getOrElse(Int.MaxValue)

      pathHelper.getPathsWithLength(
        startNodeFilter,
        relationshipFilter,
        endNodeFilter,
        direction,
        lower,
        upper
      )
    } else {
      pathHelper
        .getPathWithoutLength(startNodeFilter, relationshipFilter, endNodeFilter, direction)
        .map(
          Seq(_)
        )
    }
  }

  override def statistics: TuDBStatistics = tuDBStatistics

  private def nodeLabelNameToId(name: String): Option[Int] = {
    val nodeLabelId: Option[Int] = TuDBStoreContext.getNodeStoreAPI.getLabelId(name)
    nodeLabelId match {
      case Some(labelId) => Some(labelId)
      case None => {
        TuDBStoreContext.getNodeStoreAPI.addLabel(name)
        TuDBStoreContext.getNodeStoreAPI.getLabelId(name)
      }
    }
  }

  private def nodePropNameToId(name: String): Option[Int] = {
    val propId = TuDBStoreContext.getNodeStoreAPI.getPropertyKeyId(name)
    propId match {
      case Some(propertyId) => Some(propertyId)
      case None => {
        TuDBStoreContext.getNodeStoreAPI.addPropertyKey(name)
        TuDBStoreContext.getNodeStoreAPI.getPropertyKeyId(name)
      }
    }
  }

  def relTypeNameToId(name: String): Option[Int] =
    TuDBStoreContext.getRelationshipAPI.getRelationTypeId(name)

  private def relPropNameToId(name: String): Option[Int] = {
    val relPropId = TuDBStoreContext.getRelationshipAPI.getPropertyKeyId(name)
    relPropId match {
      case Some(propertyId) => Some(propertyId)
      case None =>
        TuDBStoreContext.getRelationshipAPI.addPropertyKey(name)
        TuDBStoreContext.getRelationshipAPI.getPropertyKeyId(name)
    }
  }

  def findOutRelations(
      fromNodeId: Long,
      edgeType: Option[Int] = None
    ): Iterator[StoredRelationship] = {
    TuDBStoreContext.getRelationshipAPI.findOutRelations(fromNodeId, edgeType)
  }
  def findInRelations(
      endNodeId: Long,
      edgeType: Option[Int] = None
    ): Iterator[StoredRelationship] = {
    TuDBStoreContext.getRelationshipAPI.findInRelations(endNodeId, edgeType)
  }

  private def mapLynxNodeLabel(id: Int): LynxNodeLabel =
    TuDBStoreContext.getNodeStoreAPI
      .getLabelName(id)
      .map(LynxNodeLabel)
      .getOrElse(LynxNodeLabel("unknown"))

  private def mapLynxRelationshipType(id: Int): LynxRelationshipType =
    TuDBStoreContext.getRelationshipAPI
      .getRelationTypeName(id)
      .map(LynxRelationshipType)
      .getOrElse(LynxRelationshipType("unknown"))

  private def mapLynxPropKeyOfNodes(id: Int): LynxPropertyKey =
    TuDBStoreContext.getNodeStoreAPI
      .getPropertyKeyName(id)
      .map(LynxPropertyKey)
      .getOrElse(LynxPropertyKey("unknown"))

  private def mapLynxPropKeyOfRelationships(id: Int): LynxPropertyKey =
    TuDBStoreContext.getRelationshipAPI
      .getPropertyKeyName(id)
      .map(LynxPropertyKey)
      .getOrElse(LynxPropertyKey("unknown"))

  private def addNode(id: Option[Long], labels: Seq[String], nodeProps: Map[String, Any]): Long = {
    val nodeId = id.getOrElse(TuDBStoreContext.getNodeStoreAPI.newNodeId())
    val labelIds = labels.map(TuDBStoreContext.getNodeStoreAPI.addLabel).toArray
    val props: Map[Int, Any] =
      nodeProps.map(kv => (TuDBStoreContext.getNodeStoreAPI.addPropertyKey(kv._1), kv._2))
    TuDBStoreContext.getNodeStoreAPI.addNode(nodeId, labelIds, props)
    tuDBStatistics.increaseNodeCount(1)
    labelIds.foreach(tuDBStatistics.increaseNodeLabelCount(_, 1))
    nodeId
  }

  private def addRelation(
      id: Option[Long],
      label: String,
      from: Long,
      to: Long,
      relProps: Map[String, Any]
    ): Long = {
    val rid = id.getOrElse(TuDBStoreContext.getRelationshipAPI.newRelationId())
    val typeId = TuDBStoreContext.getRelationshipAPI.addRelationType(label)
    val props = relProps.map {
      case (key, value) =>
        (TuDBStoreContext.getRelationshipAPI.addPropertyKey(key), value)
    }
    //    val rel = new StoredRelationshipWithProperty(rid, from, to, labelId, props)
    TuDBStoreContext.getRelationshipAPI.addRelationship(rid, from, to, typeId, props)
    tuDBStatistics.increaseRelationCount(1)
    tuDBStatistics.increaseRelationTypeCount(typeId, 1)
    rid
  }

  protected def mapNode(node: StoredNode): TuNode = {
    TuNode(
      node.id,
      node.labelIds.map(mapLynxNodeLabel).toSeq,
      node.properties.toSeq.map {
        case (keyId, value) =>
          (mapLynxPropKeyOfNodes(keyId).value, LynxValue(value))
      }
    )
  }

  protected def mapRelation(rel: StoredRelationship): TuRelationship = {
    TuRelationship(
      rel.id,
      rel.from,
      rel.to,
      TuDBStoreContext.getRelationshipAPI.getRelationTypeName(rel.typeId).map(LynxRelationshipType),
      rel.properties.toSeq.map {
        case (keyId, value) =>
          (mapLynxPropKeyOfRelationships(keyId).value, LynxValue(value))
      }
    )
  }

  def nodeAt(id: Long): Option[TuNode] =
    TuDBStoreContext.getNodeStoreAPI.getNodeById(id).map(mapNode)

  def nodeAt(lynxId: LynxId): Option[TuNode] = nodeAt(
    lynxId.toLynxInteger.value
  )

  def relationshipAt(id: Long): Option[TuRelationship] =
    TuDBStoreContext.getRelationshipAPI.getRelationById(id).map(mapRelation)

  def relationshipAt(lynxId: LynxId): Option[TuRelationship] =
    relationshipAt(lynxId.toLynxInteger.value)

  implicit def lynxId2NodeId(lynxId: LynxId): LynxNodeId = LynxNodeId(
    lynxId.value.asInstanceOf[Long]
  )

  implicit def lynxId2RelationshipId(lynxId: LynxId): LynxRelationshipId =
    LynxRelationshipId(lynxId.value.asInstanceOf[Long])

  /** An WriteTask object needs to be returned.
    * There is no default implementation, you must override it.
    * @return The WriteTask object
    */
  override def write: WriteTask = this._writeTask

  /** All nodes.
    *
    * @return An Iterator of all nodes.
    */
  override def nodes(): Iterator[LynxNode] =
    TuDBStoreContext.getNodeStoreAPI.allNodes().map(mapNode).asInstanceOf[Iterator[LynxNode]]

  /** Filter nodes based on conditions
    * can use index engine  speed up property filter
    * @see [[org.grapheco.tudb.store.index.IndexServer]]
    * @param nodeFilter
    * @return
    */
  override def nodes(nodeFilter: NodeFilter): Iterator[LynxNode] = {
    //ugly impl: The getOrElse(-1) and filter(labelId > 0) is to avoid querying a unexisting label.
    var indexData: Iterator[LynxNode] = null
    //if has index engine and  need filter property , use index filter
    if (TuDBStoreContext.getNodeStoreAPI.hasIndex()) {
      if (nodeFilter.properties.nonEmpty) { //use index
        indexData = nodeFilter.properties
          .map(property =>
            TuDBStoreContext.getNodeStoreAPI.getNodeIdByProperty(
              TuDBStoreContext.getNodeStoreAPI.getPropertyKeyId(property._1.value).getOrElse(0),
              property._2.value
            )
          )
          .flatten
          .map(nodeId => TuDBStoreContext.getNodeStoreAPI.getNodeById(nodeId).map(mapNode))
          .filter(_.nonEmpty)
          .map(_.get.asInstanceOf[LynxNode])
          .filter(tuNode => nodeFilter.matches(tuNode))
          .iterator
      }
    }
    if (indexData != null) indexData
    else { // else load all data and filter it
      val labelIds: Seq[Int] = nodeFilter.labels
        .map(lynxNodeLabel =>
          TuDBStoreContext.getNodeStoreAPI.getLabelId(lynxNodeLabel.value).getOrElse(-1)
        )
      if (labelIds.isEmpty) {
        //  TODO: could use index without label?
        nodes().filter(p => nodeFilter.matches(p)) // no label, scan db to filter properties.
      } else if (labelIds.contains(-1)) {
        Iterator.empty // the label not exist in db
      } else {
        // get min label
        val minLabelId =
          labelIds.map(lId => statistics.getNodeLabelCount(lId).get -> lId).minBy(f => f._1)._2
        TuDBStoreContext.getNodeStoreAPI
          .getNodesByLabel(minLabelId)
          .map(mapNode)
          .filter(tuNode => nodeFilter.matches(tuNode))
      }
    }
  }

  /** Return all relationships as PathTriple.
    *
    * @return An Iterator of PathTriple
    */
  override def relationships(): Iterator[PathTriple] =
    TuDBStoreContext.getRelationshipAPI
      .allRelationsWithProperty()
      .map(rel =>
        PathTriple(
          nodeAt(rel.from).get.asInstanceOf[LynxNode],
          mapRelation(rel),
          nodeAt(rel.to).get
        )
      )

  private val _writeTask: WriteTask = new WriteTask {

    override def createElements[T](
        nodesInput: Seq[(String, NodeInput)],
        relationshipsInput: Seq[(String, RelationshipInput)],
        onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T
      ): T = {
      val nodesMap: Map[String, TuNode] = nodesInput.map {
        case (valueName, input) =>
          valueName ->
            TuNode(
              TuDBStoreContext.getNodeStoreAPI.newNodeId(),
              input.labels,
              input.props.map(kv => (kv._1.value, kv._2))
            )
      }.toMap

      def localNodeRef(ref: NodeInputRef): LynxNodeId = ref match {
        case StoredNodeInputRef(id)            => id
        case ContextualNodeInputRef(valueName) => nodesMap(valueName).id
      }

      val relationshipsMap: Map[String, TuRelationship] =
        relationshipsInput.map {
          case (valueName, input) =>
            valueName -> TuRelationship(
              TuDBStoreContext.getRelationshipAPI.newRelationId(),
              localNodeRef(input.startNodeRef).value,
              localNodeRef(input.endNodeRef).value,
              input.types.headOption,
              input.props.map(kv => (kv._1.value, kv._2))
            )
        }.toMap

      nodesMap.foreach { node =>
        addNode(
          Some(node._2.longId),
          node._2.labels.map(_.value),
          node._2.props.toMap.mapValues(_.value)
        )
      }

      relationshipsMap.foreach { rel =>
        addRelation(
          Some(rel._2.id.toLynxInteger.value),
          rel._2.relationType.get.value,
          rel._2.startId,
          rel._2.endId,
          rel._2.properties.mapValues(_.value)
        )
      }
      onCreated(nodesMap.toSeq, relationshipsMap.toSeq)
    }

    override def deleteRelations(ids: Iterator[LynxId]): Unit = ids.foreach { id =>
      TuDBStoreContext.getRelationshipAPI.deleteRelation(id.value.asInstanceOf[Long])
    }

    override def deleteNodes(ids: Seq[LynxId]): Unit =
      TuDBStoreContext.getNodeStoreAPI.deleteNodes(ids.map(_.value.asInstanceOf[Long]).iterator)

    override def setNodesProperties(
        nodeIds: Iterator[LynxId],
        data: Array[(LynxPropertyKey, Any)],
        cleanExistProperties: Boolean
      ): Iterator[Option[LynxNode]] = nodeIds.map { id =>
      data.foreach {
        case (key, value) =>
          TuDBStoreContext.getNodeStoreAPI.nodeSetProperty(
            id.toLynxInteger.value,
            nodePropNameToId(key.value).get,
            value
          )
      }
      TuDBStoreContext.getNodeStoreAPI.getNodeById(id.toLynxInteger.v).map(mapNode)
    }

    override def setNodesLabels(
        nodeIds: Iterator[LynxId],
        labels: Array[LynxNodeLabel]
      ): Iterator[Option[LynxNode]] = nodeIds.map { id =>
      labels.foreach { label =>
        TuDBStoreContext.getNodeStoreAPI.nodeAddLabel(
          id.toLynxInteger.value,
          nodeLabelNameToId(label.value).get
        )
      }
      TuDBStoreContext.getNodeStoreAPI.getNodeById(id.toLynxInteger.v).map(mapNode)
    }

    override def setRelationshipsProperties(
        relationshipIds: Iterator[LynxId],
        data: Array[(LynxPropertyKey, Any)]
      ): Iterator[Option[LynxRelationship]] = relationshipIds.map { id =>
      data.foreach {
        case (key, value) =>
          TuDBStoreContext.getRelationshipAPI.relationSetProperty(
            id.toLynxInteger.value,
            relPropNameToId(key.value).get,
            value
          )
      }
      TuDBStoreContext.getRelationshipAPI.getRelationById(id.toLynxInteger.value).map(mapRelation)
    }

    override def setRelationshipsType(
        relationshipIds: Iterator[LynxId],
        typeName: LynxRelationshipType
      ): Iterator[Option[LynxRelationship]] = relationshipIds.map { id =>
      TuDBStoreContext.getRelationshipAPI.getRelationById(id.toLynxInteger.value).map(mapRelation)
    }

    override def removeNodesProperties(
        nodeIds: Iterator[LynxId],
        data: Array[LynxPropertyKey]
      ): Iterator[Option[LynxNode]] = nodeIds.map { id =>
      data.foreach { key =>
        TuDBStoreContext.getNodeStoreAPI.nodeRemoveProperty(
          id.toLynxInteger.value,
          nodePropNameToId(key.value).get
        )
      }
      TuDBStoreContext.getNodeStoreAPI.getNodeById(id.toLynxInteger.v).map(mapNode)
    }

    override def removeNodesLabels(
        nodeIds: Iterator[LynxId],
        labels: Array[LynxNodeLabel]
      ): Iterator[Option[LynxNode]] = nodeIds.map { id =>
      labels.foreach { label =>
        TuDBStoreContext.getNodeStoreAPI.nodeRemoveLabel(
          id.toLynxInteger.value,
          nodeLabelNameToId(label.value).get
        )
      }
      TuDBStoreContext.getNodeStoreAPI.getNodeById(id.toLynxInteger.v).map(mapNode)
    }

    override def removeRelationshipsProperties(
        relationshipIds: Iterator[LynxId],
        data: Array[LynxPropertyKey]
      ): Iterator[Option[LynxRelationship]] = relationshipIds.map { id =>
      data.foreach { key =>
        TuDBStoreContext.getRelationshipAPI.relationRemoveProperty(
          id.toLynxInteger.value,
          relPropNameToId(key.value).get
        )
      }
      TuDBStoreContext.getRelationshipAPI.getRelationById(id.toLynxInteger.value).map(mapRelation)
    }

    override def removeRelationshipsType(
        relationshipIds: Iterator[LynxId],
        typeName: LynxRelationshipType
      ): Iterator[Option[LynxRelationship]] = relationshipIds.map { id =>
      // fixme
      TuDBStoreContext.getRelationshipAPI.getRelationById(id.toLynxInteger.value).map(mapRelation)
    }

    /** Commit write tasks. It is called at the end of the statement.
      *
      * @return Is it successful?
      */
    override def commit: Boolean = {
      // need flush?
      true
    }
  }

  private val runner: CypherRunner = new CypherRunner(this)

  def cypher(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult =
    runner.run(query, param)

  def close(): Unit = {
    TuDBStoreContext.getNodeStoreAPI.close()
    TuDBStoreContext.getRelationshipAPI.close()
    tuDBStatistics.close()
  }
}
