package org.grapheco.tudb.facade

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx._
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.structural._
import org.grapheco.tudb.commons.{HopUtils, PathUtils}
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
      nodeId: LynxId,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection
    ): Iterator[PathTriple] = {
    // TODO:  use properties in endNodeFilter to index node.
    // The expand relationship may not contain the relationship type,
    // or may query like this:
    // 1. (a)-[r:FRIEND]->(b)
    // 2. (a)-[r:FRIEND|KNOW]->(b)
    val relationshipTypes = relationshipFilter.types.map(t => t.value)
    if (relationshipTypes.nonEmpty) {
      relationshipTypes
        .flatMap(rType => {
          (direction match {
            case SemanticDirection.OUTGOING =>
              findOutRelations(nodeId.toLynxInteger.value, relationStore.getRelationTypeId(rType))
                .map(r => PathTriple(nodeAt(r.from).get, relationshipAt(r.id).get, nodeAt(r.to).get)
                )
            case SemanticDirection.INCOMING =>
              findInRelations(nodeId.toLynxInteger.value, relationStore.getRelationTypeId(rType))
                .map(r =>
                  PathTriple(nodeAt(r.to).get, relationshipAt(r.id).get, nodeAt(r.from).get, true)
                )
            case SemanticDirection.BOTH =>
              findOutRelations(nodeId.toLynxInteger.value, relationStore.getRelationTypeId(rType))
                .map(r => PathTriple(nodeAt(r.from).get, relationshipAt(r.id).get, nodeAt(r.to).get)
                ) ++
                findInRelations(nodeId.toLynxInteger.value, relationStore.getRelationTypeId(rType))
                  .map(r =>
                    PathTriple(nodeAt(r.to).get, relationshipAt(r.id).get, nodeAt(r.from).get, true)
                  )
          }).filter(p => endNodeFilter.matches(p.endNode))
        })
        .toIterator
    } else {
      (direction match {
        case SemanticDirection.OUTGOING =>
          findOutRelations(nodeId.toLynxInteger.value).map(r =>
            PathTriple(nodeAt(r.from).get, relationshipAt(r.id).get, nodeAt(r.to).get)
          )
        case SemanticDirection.INCOMING =>
          findInRelations(nodeId.toLynxInteger.value).map(r =>
            PathTriple(nodeAt(r.to).get, relationshipAt(r.id).get, nodeAt(r.from).get, true)
          )
        case SemanticDirection.BOTH =>
          findOutRelations(nodeId.toLynxInteger.value).map(r =>
            PathTriple(nodeAt(r.from).get, relationshipAt(r.id).get, nodeAt(r.to).get)
          ) ++
            findInRelations(nodeId.toLynxInteger.value).map(r =>
              PathTriple(nodeAt(r.to).get, relationshipAt(r.id).get, nodeAt(r.from).get, true)
            )
      }).filter(p => endNodeFilter.matches(p.endNode))
    }
  }

  /*
    The default paths will scan all the relationships then use filter to get the data we want,
    and it cannot deal with the situation of relationship with variable length,
    the cypher like (a)-[r:TYPE*1..3]->(b).

    But in TuDB, we have the relationship-index (findInRelationship and findOutRelationship),
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

    if (upperLimit.isDefined || lowerLimit.isDefined) {
      val lower = lowerLimit.getOrElse(1)
      val upper = upperLimit.getOrElse(Int.MaxValue)

      getPathsWithLength(
        startNodeFilter,
        relationshipFilter,
        endNodeFilter,
        direction,
        lower,
        upper
      )
    } else {
      getPathWithoutLength(startNodeFilter, relationshipFilter, endNodeFilter, direction).map(
        Seq(_)
      )
    }
  }

  // logic like pathWithLength
  private def getPathWithoutLength(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      endNodeFilter: NodeFilter,
      direction: SemanticDirection
    ): Iterator[PathTriple] = {
    direction match {
      case SemanticDirection.OUTGOING => {
        val pathUtils = new PathUtils(this)

        val startNodes = nodes(startNodeFilter)
        startNodes
          .flatMap(startNode => pathUtils.getSingleNodeOutgoingPaths(startNode, relationshipFilter))
          .filter(p => endNodeFilter.matches(p.endNode()))
          .flatMap(f => f.pathTriples)
      }
      case SemanticDirection.INCOMING => {
        val inComingPathUtils = new PathUtils(this)

        nodes(startNodeFilter)
          .flatMap(startNode =>
            inComingPathUtils.getSingleNodeIncomingPaths(startNode, relationshipFilter)
          )
          .filter(p => endNodeFilter.matches(p.endNode()))
          .flatMap(f => f.pathTriples)
      }
      case SemanticDirection.BOTH => {
        val pathUtils = new PathUtils(this)

        val startNodes = nodes(startNodeFilter).duplicate
        (startNodes._1
          .flatMap(startNode => pathUtils.getSingleNodeOutgoingPaths(startNode, relationshipFilter))
          .filter(p => endNodeFilter.matches(p.endNode())) ++
          startNodes._2
            .flatMap(endNode => pathUtils.getSingleNodeIncomingPaths(endNode, relationshipFilter))
            .filter(p => endNodeFilter.matches(p.endNode()))).flatMap(f => f.pathTriples)

      }
    }
  }

  /**  minHops and maxHops are optional and default to 1 and infinity respectively.
    *
    *  If the path length between two nodes is zero, they are by definition the same node.
    *  Note that when matching zero length paths the result may contain a match
    *  even when matching on a relationship type not in use.
    *
    * [:TYPE*minHops..maxHops] = query fixed range relationships
    * [:TYPE*minHops..]  = query relationships from minHops to INF
    * [:TYPE*..maxHops]  = query relationships from 1 to maxHops
    * [:TYPE*Hops] = query fixed length relationships
    *
    *  p = PathTriple(START_NODE, RELATIONSHIP, END_NODE)
    *      eg: match (n: Person)-[r:TYPE*1..3]->(m: Person)
    *      hop1 ==>   Seq( Seq(p1), Seq(p2), Seq(p3), Seq(p4), Seq(p5) ) // five single relationships
    *      hop2 ==>   Seq( Seq(p1, p2), Seq(p3, p4) ) // two hop-2 relationships
    *      hop3 ==>   Seq( Seq(p1, p2, p5) ) // one hop-3 relationships
    *
    *      Total: hop1 ++ hop2 ++ hop3 =
    *         Seq(
    *              Seq( Seq(p1), Seq(p2), Seq(p3), Seq(p4), Seq(p5) ),
    *              Seq( Seq(p1, p2), Seq(p3, p4) ),
    *              Seq( Seq(p1, p2, p5) )
    *            )
    *
    *            TODO: Check circle
    */
  private def getPathsWithLength(
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
        val hopsToLowerLimit =
          initInOutStartHop(startNodeFilter, relationshipFilter, direction, lowerLimit)
        val hops = getInComingLengthPaths(
          hopsToLowerLimit,
          relationshipFilter,
          endNodeFilter,
          lowerLimit,
          upperLimit
        )
        val r1 = hops.map(hop => hop.paths.map(path => path.pathTriples))
        r1.foldLeft(Seq.empty[Seq[PathTriple]])((a, b) => a ++ b).toIterator
      }
      case SemanticDirection.BOTH => {
        // different from incoming or outgoing, expand from each node, we all need to find in and out relationships
        val hopsToLowerLimit = initBothStartHop(startNodeFilter, relationshipFilter, lowerLimit)
        val hops = getBothLengthPaths(
          hopsToLowerLimit,
          relationshipFilter,
          startNodeFilter,
          endNodeFilter,
          lowerLimit,
          upperLimit
        )
        val r1 = hops.map(hop => hop.paths.map(path => path.pathTriples))
        r1.foldLeft(Seq.empty[Seq[PathTriple]])((a, b) => a ++ b).toIterator
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

    val hopUtils = new HopUtils(new PathUtils(this))

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
      nextHop = hopUtils.getNextOutGoingHop(nextHop, relationshipFilter)
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
            getInComingHopFromOne2Limit(beginNodes, relationshipFilter, lowerLimit)
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
    val pathUtils = new PathUtils(this)
    val hopUtils = new HopUtils(pathUtils)

    val collected: ArrayBuffer[GraphHop] = ArrayBuffer.empty

    val firstHop = GraphHop(
      beginNodes
        .flatMap(node => pathUtils.getSingleNodeOutgoingPaths(node, relationshipFilter))
        .toSeq
        .filter(p => p.pathTriples.nonEmpty)
    )

    collected.append(firstHop)
    var nextHop: GraphHop = null

    // Iterator to reach lowerLimit.
    var count = 1
    while (count < lowerLimit) {
      count += 1
      nextHop = hopUtils.getNextOutGoingHop(firstHop, relationshipFilter)
      collected.append(nextHop)
    }
    collected
  }

  // Same logic as getOutGoingLengthPath.
  def getInComingLengthPaths(
      hopsToLowerLimit: Seq[GraphHop],
      relationshipFilter: RelationshipFilter,
      stopNodeFilter: NodeFilter,
      lowerLimit: Int,
      upperLimit: Int
    ): Seq[GraphHop] = {
    val hopUtils = new HopUtils(new PathUtils(this))
    val collectedResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val filteredResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty

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
    while (count <= upperLimit && flag) {
      count += 1
      nextHop = hopUtils.getNextInComingHop(nextHop, relationshipFilter)
      if (nextHop.paths.nonEmpty) {
        collectedResult.append(nextHop)
      } else flag = false
    }
    collectedResult.foreach(hops => {
      val filteredPaths =
        hops.paths.filter(path => stopNodeFilter.matches(path.pathTriples.last.endNode))
      filteredResult.append(GraphHop(filteredPaths))
    })

    filteredResult
  }

  // Same logic as getOutGoingHopFromOne2Limit
  private def getInComingHopFromOne2Limit(
      beginNodes: Iterator[LynxNode],
      relationshipFilter: RelationshipFilter,
      lowerLimit: Int
    ): ArrayBuffer[GraphHop] = {
    val pathUtils = new PathUtils(this)
    val hopUtils = new HopUtils(pathUtils)

    val collected: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val firstHop = GraphHop(
      beginNodes
        .flatMap(node => pathUtils.getSingleNodeIncomingPaths(node, relationshipFilter))
        .toSeq
        .filter(p => p.pathTriples.nonEmpty)
    )

    collected.append(firstHop)
    var nextHop: GraphHop = null
    var count = 1
    while (count < lowerLimit) {
      count += 1
      nextHop = hopUtils.getNextInComingHop(firstHop, relationshipFilter)
      collected.append(nextHop)
    }
    collected
  }

  // need both for every hop
  private def initBothStartHop(
      startNodeFilter: NodeFilter,
      relationshipFilter: RelationshipFilter,
      lowerLimit: Int
    ): Seq[GraphHop] = {
    val beginNodes = nodes(startNodeFilter)
    lowerLimit match {
      case 0 => {
        val res = beginNodes.map(node => GraphPath(Seq(PathTriple(node, null, node)))).toSeq
        Seq(GraphHop(res))
      }
      case _ => {
        getBothHopFromOne2Limit(beginNodes, relationshipFilter, lowerLimit)
      }
    }
  }

  def getBothLengthPaths(
      hopsToLowerLimit: Seq[GraphHop],
      relationshipFilter: RelationshipFilter,
      startNodeFilter: NodeFilter,
      endNodeFilter: NodeFilter,
      lowerLimit: Int,
      upperLimit: Int
    ): Seq[GraphHop] = {
    val hopUtils = new HopUtils(new PathUtils(this))

    val collectedResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val filteredResult: ArrayBuffer[GraphHop] = ArrayBuffer.empty

    // same logic as outgoing
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

    while (count <= upperLimit && flag) {
      count += 1
      nextHop = hopUtils.getNextBothHop(nextHop, relationshipFilter)
      if (nextHop.paths.nonEmpty) {
        collectedResult.append(nextHop)
      } else flag = false
    }

    /*
     Because we search both-path(no direction), so there are two situation to filter:
       1. start node match startNodeFilter and end node match endNodeFilter
       or
       2.start node match endNodeFilter and end node match startNodeFilter
     */
    collectedResult.distinct.foreach(hops => {
      val res = hops.paths.filter(thisPath =>
        startNodeFilter.matches(thisPath.pathTriples.head.startNode) && endNodeFilter.matches(
          thisPath.pathTriples.last.endNode
        )
      )
      filteredResult.append(GraphHop(res))
    })
    filteredResult
  }

  private def getBothHopFromOne2Limit(
      beginNodes: Iterator[LynxNode],
      relationshipFilter: RelationshipFilter,
      lowerLimit: Int
    ): ArrayBuffer[GraphHop] = {
    val pathUtils = new PathUtils(this)
    val hopUtils = new HopUtils(pathUtils)
    val collected: ArrayBuffer[GraphHop] = ArrayBuffer.empty
    val firstHop = GraphHop(
      beginNodes
        .flatMap(node => pathUtils.getSingleNodeBothPaths(node, relationshipFilter))
        .toSeq
        .filter(p => p.pathTriples.nonEmpty)
    )
    collected.append(firstHop)
    var nextHop: GraphHop = null
    var count = 1
    while (count < lowerLimit) {
      count += 1
      nextHop = hopUtils.getNextBothHop(firstHop, relationshipFilter)
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

  private def addNode(id: Option[Long], labels: Seq[String], nodeProps: Map[String, Any]): Long = {
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
    val props = relProps.map {
      case (key, value) =>
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
      relationStore.getRelationTypeName(rel.typeId).map(LynxRelationshipType),
      rel.properties.toSeq.map {
        case (keyId, value) =>
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
        //  TODO: could use index without label?
        nodes().filter(p => nodeFilter.matches(p)) // no label, scan db to filter properties.
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
      val nodesMap: Map[String, TuNode] = nodesInput.map {
        case (valueName, input) =>
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
        relationshipsInput.map {
          case (valueName, input) =>
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
      data.foreach {
        case (key, value) =>
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
      data.foreach {
        case (key, value) =>
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

  def cypher(query: String, param: Map[String, Any] = Map.empty[String, Any]): LynxResult =
    runner.run(query, param)

  def close(): Unit = {
    nodeStoreAPI.close()
    relationStore.close()
    tuDBStatistics.close()
  }
}
