package org.grapheco.tudb.facade

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx._
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.grapheco.tudb.commons.{OutGoingHopUtils, OutGoingPathUtils}
import org.grapheco.tudb.graph.{GraphHop, GraphPath}
import org.grapheco.tudb.store.meta.TuDBStatistics
import org.grapheco.tudb.store.node._
import org.grapheco.tudb.store.relationship._
import org.opencypher.v9_0.expressions.SemanticDirection

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

/** @ClassName GraphFacade
  * @Description Needs further impl.
  * @Author huchuan
  * @Date 2022/3/24
  * @Version 0.1
  */
class GraphFacade(
    nodeStoreAPI: NodeStoreSPI,
    relationStore: RelationStoreSPI,
    tuDBStatistics: TuDBStatistics,
    onClose: => Unit)
  extends LazyLogging
  with GraphModel {

  override def paths(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection,
      upperLimit: Option[Int],
      lowerLimit: Option[Int]
    ): Iterator[Seq[PathTriple]] = {

    if (upperLimit.isDefined || lowerLimit.isDefined) {
      val lower = lowerLimit.getOrElse(1)
      val upper = upperLimit.getOrElse(Int.MaxValue)

      processPathWithLength(
        startNodeFilter,
        relationshipFilter,
        endNodeFilter,
        direction,
        lower,
        upper
      )
    } else {
      // TODO: process pathWithoutLength
      super.paths(
        startNodeFilter,
        relationshipFilter,
        endNodeFilter,
        direction,
        upperLimit,
        lowerLimit
      )
    }
  }

  private def processPathWithLength(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection,
      lowerLimit: Int,
      upperLimit: Int
    ): Iterator[Seq[PathTriple]] = {
    direction match {
      // Outgoing relationships.
      case SemanticDirection.OUTGOING => {
        // Get hops from 1 to lowerLimit, like (a)-[r:TYPE*3..5]->(b), we should init hop to hop-3.
        val hopsToLowerLimit =
          initInOutStartHop(startNodeFilter, relationshipFilter, direction, lowerLimit)

        val hops = getOutGoingPathsWithLength(
          hopsToLowerLimit,
          relationshipFilter,
          endNodeFilter,
          lowerLimit,
          upperLimit
        )
        val r1 = hops.map(hop => hop.paths.map(path => path.pathTriples))
        r1.foldLeft(Seq.empty[Seq[PathTriple]])((a, b) => a ++ b).toIterator
      }
      case SemanticDirection.INCOMING => {
        // TODO: in latter pr
        ???
      }
      case SemanticDirection.BOTH => {
        // TODO: in latter pr
        ???
      }
    }
  }

  def getOutGoingPathsWithLength(
      hopsToLowerLimit: Seq[GraphHop],
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      lowerLimit: Int,
      upperLimit: Int
    ): Seq[GraphHop] = {

    val outGoingHopUtils = new OutGoingHopUtils(new OutGoingPathUtils(this))

    val collectedResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val filteredResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty

    // If (a)-[r:TYPE*3..5]->(b), then hopsToLowerLimit have the hops from 1 to 3, we only need to get hop3's paths
    var nextHop = {
      if (lowerLimit == 0 || lowerLimit == 1) hopsToLowerLimit.head
      else hopsToLowerLimit(lowerLimit - 1)
    }
    collectedResult.append(nextHop)
    var count = {
      if (lowerLimit == 0) 0
      else lowerLimit + 1
    }
    var flag = {
      if (upperLimit != 0) true
      else false
    }

    // Loop to reach the upperLimit, if nextHop is empty, loop stop.
    while (count <= upperLimit && flag) {
      count += 1
      nextHop = outGoingHopUtils.getNextOutGoingHop(nextHop, relationshipFilter)
      if (nextHop.paths.nonEmpty) {
        collectedResult.append(nextHop)
      } else flag = false
    }

    // Filter loop result
    collectedResult.foreach(hops => {
      val filteredPaths =
        hops.paths.filter(path => endNodeFilter.matches(path.pathTriples.last.endNode))
      filteredResult.append(GraphHop(filteredPaths))
    })

    filteredResult
  }

  private def initInOutStartHop(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      direction: SemanticDirection,
      lowerLimit: Int
    ): Seq[GraphHop] = {
    val beginNodes = nodes(startNodeFilter)
    lowerLimit match {
      // 0 means relationship to itself, like (a)-->(a)
      case 0 => {
        val res = beginNodes.map(node => GraphPath(Seq(PathTriple(node, null, node)))).toSeq
        Seq(GraphHop(res))
      }
      case _ => {
        direction match {
          case SemanticDirection.OUTGOING => {
            getOutGoingHopFromOne2Limit(beginNodes, relationshipFilter, lowerLimit)
          }
          case SemanticDirection.INCOMING => {
            // TODO: in next pr
            ???
          }
        }
      }
    }
  }

  private def getOutGoingHopFromOne2Limit(
      beginNodes: Iterator[LynxNode],
      relationshipFilter: RelationshipFilter,
      lowerLimit: Int
    ): ArrayBuffer[GraphHop] = {
    val outGoingPathUtils = new OutGoingPathUtils(this)
    val outGoingHopUtils = new OutGoingHopUtils(outGoingPathUtils)

    val collected: ArrayBuffer[GraphHop] = ArrayBuffer.empty

    val firstHop = GraphHop(
      beginNodes
        .flatMap(node =>
          outGoingPathUtils.getSingleNodeOutGoingPaths(node, relationshipFilter).pathTriples
        )
        .map(p => GraphPath(Seq(p)))
        .toSeq
    )

    collected.append(firstHop)
    var nextHop: GraphHop = null

    // Iterator to reach lowerLimit.
    var count = 1
    while (count < lowerLimit) {
      count += 1
      nextHop = outGoingHopUtils.getNextOutGoingHop(firstHop, relationshipFilter)
      collected.append(nextHop)
    }
    collected
  }

  override def statistics: TuDBStatistics = tuDBStatistics

  private def nodeLabelNameToId(name: String): Option[Int] = {
    val nodeLabelId: Option[Int] = nodeStoreAPI.getLabelId(name)
    nodeLabelId match {
      case Some(labelId) => Some(labelId)
      case None => {
        nodeStoreAPI.addLabel(name)
        nodeStoreAPI.getLabelId(name)
      }
    }
  }

  private def nodePropNameToId(name: String): Option[Int] = {
    val propId = nodeStoreAPI.getPropertyKeyId(name)
    propId match {
      case Some(propertyId) => Some(propertyId)
      case None => {
        nodeStoreAPI.addPropertyKey(name)
        nodeStoreAPI.getPropertyKeyId(name)
      }
    }
  }

  def relTypeNameToId(name: String): Option[Int] =
    relationStore.getRelationTypeId(name)

  private def relPropNameToId(name: String): Option[Int] = {
    val relPropId = relationStore.getPropertyKeyId(name)
    relPropId match {
      case Some(propertyId) => Some(propertyId)
      case None =>
        relationStore.addPropertyKey(name)
        relationStore.getPropertyKeyId(name)
    }
  }

  def findOutRelations(
      fromNodeId: Long,
      edgeType: Option[Int] = None
    ): Iterator[StoredRelationship] = {
    relationStore.findOutRelations(fromNodeId, edgeType)
  }
  def findInRelations(
      endNodeId: Long,
      edgeType: Option[Int] = None
    ): Iterator[StoredRelationship] = {
    relationStore.findInRelations(endNodeId, edgeType)
  }

  private def mapLynxNodeLabel(id: Int): LynxNodeLabel =
    nodeStoreAPI
      .getLabelName(id)
      .map(LynxNodeLabel)
      .getOrElse(LynxNodeLabel("unknown"))

  private def mapLynxRelationshipType(id: Int): LynxRelationshipType =
    relationStore
      .getRelationTypeName(id)
      .map(LynxRelationshipType)
      .getOrElse(LynxRelationshipType("unknown"))

  private def mapLynxPropKeyOfNodes(id: Int): LynxPropertyKey =
    nodeStoreAPI
      .getPropertyKeyName(id)
      .map(LynxPropertyKey)
      .getOrElse(LynxPropertyKey("unknown"))

  private def mapLynxPropKeyOfRelationships(id: Int): LynxPropertyKey =
    relationStore
      .getPropertyKeyName(id)
      .map(LynxPropertyKey)
      .getOrElse(LynxPropertyKey("unknown"))

  private def addNode(
      id: Option[Long],
      labels: Seq[String],
      nodeProps: Map[String, Any]
    ): Long = {
    val nodeId = id.getOrElse(nodeStoreAPI.newNodeId())
    val labelIds = labels.map(nodeStoreAPI.addLabel).toArray
    val props: Map[Int, Any] =
      nodeProps.map(kv => (nodeStoreAPI.addPropertyKey(kv._1), kv._2))
    nodeStoreAPI.addNode(nodeId, labelIds, props)
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
    val rid = id.getOrElse(relationStore.newRelationId())
    val typeId = relationStore.addRelationType(label)
    val props = relProps.map { case (key, value) =>
      (relationStore.addPropertyKey(key), value)
    }
//    val rel = new StoredRelationshipWithProperty(rid, from, to, labelId, props)
    relationStore.addRelationship(rid, from, to, typeId, props)
    tuDBStatistics.increaseRelationCount(1)
    tuDBStatistics.increaseRelationTypeCount(typeId, 1)
    rid
  }

  protected def mapNode(node: StoredNode): TuNode = {
    TuNode(
      node.id,
      node.labelIds.map(mapLynxNodeLabel).toSeq,
      node.properties.toSeq.map { case (keyId, value) =>
        (mapLynxPropKeyOfNodes(keyId).value, LynxValue(value))
      }
    )
  }

  protected def mapRelation(rel: StoredRelationship): TuRelationship = {
    TuRelationship(
      rel.id,
      rel.from,
      rel.to,
      relationStore.getRelationTypeName(rel.typeId).map(LynxRelationshipType),
      rel.properties.toSeq.map { case (keyId, value) =>
        (mapLynxPropKeyOfRelationships(keyId).value, LynxValue(value))
      }
    )
  }

  def nodeAt(id: Long): Option[TuNode] =
    nodeStoreAPI.getNodeById(id).map(mapNode)

  def nodeAt(lynxId: LynxId): Option[TuNode] = nodeAt(
    lynxId.toLynxInteger.value
  )

  def relationshipAt(id: Long): Option[TuRelationship] =
    relationStore.getRelationById(id).map(mapRelation)

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
    nodeStoreAPI.allNodes().map(mapNode).asInstanceOf[Iterator[LynxNode]]

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
    if (nodeStoreAPI.hasIndex()) {
      if (nodeFilter.properties.nonEmpty) { //use index
        indexData = nodeFilter.properties
          .map(property =>
            nodeStoreAPI.getNodeIdByProperty(
              nodeStoreAPI.getPropertyKeyId(property._1.value).getOrElse(0),
              property._2.value
            )
          )
          .flatten
          .map(nodeId => nodeStoreAPI.getNodeById(nodeId).map(mapNode))
          .filter(_.nonEmpty)
          .map(_.get.asInstanceOf[LynxNode])
          .filter(tuNode => nodeFilter.matches(tuNode))
          .iterator
      }
    }
    if (indexData != null) indexData
    else { // else load all data and filter it
      val labelIds: Seq[Int] = nodeFilter.labels
        .map(lynxNodeLabel => nodeStoreAPI.getLabelId(lynxNodeLabel.value).getOrElse(-1))
      if (labelIds.isEmpty) {
        nodes()
      } else if (labelIds.contains(-1)) {
        Iterator.empty // the label not exist in db
      } else {
        // get min label
        val minLabelId =
          labelIds.map(lId => statistics.getNodeLabelCount(lId).get -> lId).minBy(f => f._1)._2
        nodeStoreAPI
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
    relationStore
      .allRelations(true)
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
      val nodesMap: Map[String, TuNode] = nodesInput.map { case (valueName, input) =>
        valueName ->
          TuNode(
            nodeStoreAPI.newNodeId(),
            input.labels,
            input.props.map(kv => (kv._1.value, kv._2))
          )
      }.toMap

      def localNodeRef(ref: NodeInputRef): LynxNodeId = ref match {
        case StoredNodeInputRef(id)            => id
        case ContextualNodeInputRef(valueName) => nodesMap(valueName).id
      }

      val relationshipsMap: Map[String, TuRelationship] =
        relationshipsInput.map { case (valueName, input) =>
          valueName -> TuRelationship(
            relationStore.newRelationId(),
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
      relationStore.deleteRelation(id.value.asInstanceOf[Long])
    }

    override def deleteNodes(ids: Seq[LynxId]): Unit =
      nodeStoreAPI.deleteNodes(ids.map(_.value.asInstanceOf[Long]).iterator)

    override def setNodesProperties(
        nodeIds: Iterator[LynxId],
        data: Array[(LynxPropertyKey, Any)],
        cleanExistProperties: Boolean
      ): Iterator[Option[LynxNode]] = nodeIds.map { id =>
      data.foreach { case (key, value) =>
        nodeStoreAPI.nodeSetProperty(
          id.toLynxInteger.value,
          nodePropNameToId(key.value).get,
          value
        )
      }
      nodeStoreAPI.getNodeById(id.toLynxInteger.v).map(mapNode)
    }

    override def setNodesLabels(
        nodeIds: Iterator[LynxId],
        labels: Array[LynxNodeLabel]
      ): Iterator[Option[LynxNode]] = nodeIds.map { id =>
      labels.foreach { label =>
        nodeStoreAPI.nodeAddLabel(
          id.toLynxInteger.value,
          nodeLabelNameToId(label.value).get
        )
      }
      nodeStoreAPI.getNodeById(id.toLynxInteger.v).map(mapNode)
    }

    override def setRelationshipsProperties(
        relationshipIds: Iterator[LynxId],
        data: Array[(LynxPropertyKey, Any)]
      ): Iterator[Option[LynxRelationship]] = relationshipIds.map { id =>
      data.foreach { case (key, value) =>
        relationStore.relationSetProperty(
          id.toLynxInteger.value,
          relPropNameToId(key.value).get,
          value
        )
      }
      relationStore.getRelationById(id.toLynxInteger.value).map(mapRelation)
    }

    override def setRelationshipsType(
        relationshipIds: Iterator[LynxId],
        typeName: LynxRelationshipType
      ): Iterator[Option[LynxRelationship]] = relationshipIds.map { id =>
      relationStore.getRelationById(id.toLynxInteger.value).map(mapRelation)
    }

    override def removeNodesProperties(
        nodeIds: Iterator[LynxId],
        data: Array[LynxPropertyKey]
      ): Iterator[Option[LynxNode]] = nodeIds.map { id =>
      data.foreach { key =>
        nodeStoreAPI.nodeRemoveProperty(
          id.toLynxInteger.value,
          nodePropNameToId(key.value).get
        )
      }
      nodeStoreAPI.getNodeById(id.toLynxInteger.v).map(mapNode)
    }

    override def removeNodesLabels(
        nodeIds: Iterator[LynxId],
        labels: Array[LynxNodeLabel]
      ): Iterator[Option[LynxNode]] = nodeIds.map { id =>
      labels.foreach { label =>
        nodeStoreAPI.nodeRemoveLabel(
          id.toLynxInteger.value,
          nodeLabelNameToId(label.value).get
        )
      }
      nodeStoreAPI.getNodeById(id.toLynxInteger.v).map(mapNode)
    }

    override def removeRelationshipsProperties(
        relationshipIds: Iterator[LynxId],
        data: Array[LynxPropertyKey]
      ): Iterator[Option[LynxRelationship]] = relationshipIds.map { id =>
      data.foreach { key =>
        relationStore.relationRemoveProperty(
          id.toLynxInteger.value,
          relPropNameToId(key.value).get
        )
      }
      relationStore.getRelationById(id.toLynxInteger.value).map(mapRelation)
    }

    override def removeRelationshipsType(
        relationshipIds: Iterator[LynxId],
        typeName: LynxRelationshipType
      ): Iterator[Option[LynxRelationship]] = relationshipIds.map { id =>
      // fixme
      relationStore.getRelationById(id.toLynxInteger.value).map(mapRelation)
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

  def cypher(
      query: String,
      param: Map[String, Any] = Map.empty[String, Any]
    ): LynxResult = runner.run(query, param)

  def close(): Unit = {
    nodeStoreAPI.close()
    relationStore.close()
    tuDBStatistics.close()
  }
}
