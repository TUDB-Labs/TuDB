package org.grapheco.lynx.operator

import org.grapheco.lynx.procedure.DefaultProcedureRegistry
import org.grapheco.lynx.procedure.functions.{AggregatingFunctions, ListFunctions, LogarithmicFunctions, NumericFunctions, PredicateFunctions, ScalarFunctions, StringFunctions, TimeFunctions, TrigonometricFunctions}
import org.grapheco.lynx.{CachedQueryParser, ContextualNodeInputRef, CypherRunnerContext, DataFrameOperator, DefaultDataFrameOperator, DefaultExpressionEvaluator, DefaultQueryParser, ExecutionContext, ExecutionOperator, ExpressionEvaluator, GraphModel, Index, IndexManager, NodeFilter, NodeInput, NodeInputRef, PathTriple, PhysicalPlannerContext, QueryParser, RelationshipInput, RowBatch, Statistics, StoredNodeInputRef, WriteTask}
import org.grapheco.lynx.types.{DefaultTypeSystem, LynxValue}
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.opencypher.v9_0.expressions.{Expression, LabelName, MapExpression, NodePattern, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.InputPosition
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  *@author:John117
  *@createDate:2022/7/30
  *@description:
  */
class BaseOperatorTest {
  val defaultPosition = InputPosition(0, 0, 0)

  val typeSystem = new DefaultTypeSystem()
  val procedure = new DefaultProcedureRegistry(
    typeSystem,
    classOf[AggregatingFunctions],
    classOf[ListFunctions],
    classOf[LogarithmicFunctions],
    classOf[NumericFunctions],
    classOf[PredicateFunctions],
    classOf[ScalarFunctions],
    classOf[StringFunctions],
    classOf[TimeFunctions],
    classOf[TrigonometricFunctions]
  )
  val expressionEvaluator: ExpressionEvaluator =
    new DefaultExpressionEvaluator(model, typeSystem, procedure)

  val runnerContext =
    CypherRunnerContext(typeSystem, procedure, null, expressionEvaluator, model)

  val ctx = ExecutionContext(null, null, Map.empty)

  case class TestId(value: Long) extends LynxId {
    override def toLynxInteger: LynxInteger = LynxInteger(value)
  }
  case class TestNode(
      id: TestId,
      labels: Seq[LynxNodeLabel],
      props: Map[LynxPropertyKey, LynxValue])
    extends LynxNode {
    override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)

    override def keys: Seq[LynxPropertyKey] = props.keys.toSeq
  }

  case class TestRelationship(
      id: TestId,
      startNodeId: TestId,
      endNodeId: TestId,
      relationType: Option[LynxRelationshipType],
      props: Map[LynxPropertyKey, LynxValue])
    extends LynxRelationship {
    override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = props.get(propertyKey)
    override def keys: Seq[LynxPropertyKey] = props.keys.toSeq
  }

  case class TestPathTriple(leftNode: TestNode, rel: TestRelationship, rightNode: TestNode) {}

  val all_nodes: mutable.ArrayBuffer[TestNode] = mutable.ArrayBuffer()

  val all_rels: mutable.ArrayBuffer[TestRelationship] = mutable.ArrayBuffer()

  var _nodeId: Long = 0

  var _relationshipId: Long = 0

  val _index: mutable.ArrayBuffer[Index] = mutable.ArrayBuffer()

  val model: GraphModel = new GraphModel {

    private def nodeId: TestId = { _nodeId += 1; TestId(_nodeId) }

    private def relationshipId: TestId = { _relationshipId += 1; TestId(_relationshipId) }

    private def nodeAt(id: LynxId): Option[TestNode] = all_nodes.find(_.id == id)

    private def relationshipAt(id: LynxId): Option[TestRelationship] = all_rels.find(_.id == id)

    implicit def lynxId2myId(lynxId: LynxId): TestId = TestId(lynxId.value.asInstanceOf[Long])

    private val _writeTask: WriteTask = new WriteTask {

      val _nodesBuffer: mutable.Map[TestId, TestNode] = mutable.Map()

      val _nodesToDelete: mutable.ArrayBuffer[TestId] = mutable.ArrayBuffer()

      val _relationshipsBuffer: mutable.Map[TestId, TestRelationship] = mutable.Map()

      val _relationshipsToDelete: mutable.ArrayBuffer[TestId] = mutable.ArrayBuffer()

      private def updateNodes(
          ids: Iterator[LynxId],
          update: TestNode => TestNode
        ): Iterator[Option[LynxNode]] = {
        ids.map { id =>
          val updated = _nodesBuffer.get(id).orElse(nodeAt(id)).map(update)
          updated.foreach(newNode => _nodesBuffer.update(newNode.id, newNode))
          updated
        }
      }

      private def updateRelationships(
          ids: Iterator[LynxId],
          update: TestRelationship => TestRelationship
        ): Iterator[Option[TestRelationship]] = {
        ids.map { id =>
          val updated = _relationshipsBuffer.get(id).orElse(relationshipAt(id)).map(update)
          updated.foreach(newRel => _relationshipsBuffer.update(newRel.id, newRel))
          updated
        }
      }

      override def createElements[T](
          nodesInput: Seq[(String, NodeInput)],
          relationshipsInput: Seq[(String, RelationshipInput)],
          onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T
        ): T = {
        val nodesMap: Map[String, TestNode] = nodesInput.map {
          case (valueName, input) => valueName -> TestNode(nodeId, input.labels, input.props.toMap)
        }.toMap

        def localNodeRef(ref: NodeInputRef): TestId = ref match {
          case StoredNodeInputRef(id)            => id
          case ContextualNodeInputRef(valueName) => nodesMap(valueName).id
        }

        val relationshipsMap: Map[String, TestRelationship] = relationshipsInput.map {
          case (valueName, input) =>
            valueName -> TestRelationship(
              relationshipId,
              localNodeRef(input.startNodeRef),
              localNodeRef(input.endNodeRef),
              input.types.headOption,
              input.props.toMap
            )
        }.toMap

        _nodesBuffer ++= nodesMap.map { case (_, node) => (node.id, node) }
        _relationshipsBuffer ++= relationshipsMap.map {
          case (_, relationship) => (relationship.id, relationship)
        }
        onCreated(nodesMap.toSeq, relationshipsMap.toSeq)
      }

      override def deleteRelations(ids: Iterator[LynxId]): Unit = ids.foreach { id =>
        _relationshipsBuffer.remove(id)
        _relationshipsToDelete += id
      }

      override def deleteNodes(ids: Seq[LynxId]): Unit = ids.foreach { id =>
        _nodesBuffer.remove(id)
        _nodesToDelete += id
      }

      override def setNodesProperties(
          nodeIds: Iterator[LynxId],
          data: Array[(LynxPropertyKey, Any)],
          cleanExistProperties: Boolean
        ): Iterator[Option[LynxNode]] =
        updateNodes(
          nodeIds,
          old =>
            TestNode(
              old.id,
              old.labels,
              if (cleanExistProperties) Map.empty
              else old.props ++ data.toMap.mapValues(LynxValue.apply)
            )
        )

      override def setNodesLabels(
          nodeIds: Iterator[LynxId],
          labels: Array[LynxNodeLabel]
        ): Iterator[Option[LynxNode]] =
        updateNodes(
          nodeIds,
          old => TestNode(old.id, (old.labels ++ labels.toSeq).distinct, old.props)
        )

      override def setRelationshipsProperties(
          relationshipIds: Iterator[LynxId],
          data: Array[(LynxPropertyKey, Any)]
        ): Iterator[Option[LynxRelationship]] =
        updateRelationships(
          relationshipIds,
          old =>
            TestRelationship(
              old.id,
              old.startNodeId,
              old.endNodeId,
              old.relationType,
              data.toMap.mapValues(LynxValue.apply)
            )
        )

      override def setRelationshipsType(
          relationshipIds: Iterator[LynxId],
          typeName: LynxRelationshipType
        ): Iterator[Option[LynxRelationship]] =
        updateRelationships(
          relationshipIds,
          old => TestRelationship(old.id, old.startNodeId, old.endNodeId, Some(typeName), old.props)
        )

      override def removeNodesProperties(
          nodeIds: Iterator[LynxId],
          data: Array[LynxPropertyKey]
        ): Iterator[Option[LynxNode]] =
        updateNodes(
          nodeIds,
          old => TestNode(old.id, old.labels, old.props.filterNot(p => data.contains(p._1)))
        )

      override def removeNodesLabels(
          nodeIds: Iterator[LynxId],
          labels: Array[LynxNodeLabel]
        ): Iterator[Option[LynxNode]] =
        updateNodes(
          nodeIds,
          old => TestNode(old.id, old.labels.filterNot(labels.contains), old.props)
        )

      override def removeRelationshipsProperties(
          relationshipIds: Iterator[LynxId],
          data: Array[LynxPropertyKey]
        ): Iterator[Option[LynxRelationship]] =
        updateRelationships(
          relationshipIds,
          old =>
            TestRelationship(
              old.id,
              old.startNodeId,
              old.endNodeId,
              old.relationType,
              old.props.filterNot(p => data.contains(p._1))
            )
        )

      override def removeRelationshipsType(
          relationshipIds: Iterator[LynxId],
          typeName: LynxRelationshipType
        ): Iterator[Option[LynxRelationship]] =
        updateRelationships(
          relationshipIds,
          old => TestRelationship(old.id, old.startNodeId, old.endNodeId, None, old.props)
        )

      override def commit: Boolean = {
        val index_nodes = all_nodes.map(_.id.value)
        val index_relationships = all_rels.map(_.id.value)
        this._nodesBuffer.toArray
          .sortBy(_._1.value)
          .map {
            case (id, node) => (index_nodes.indexOf(id.value), node)
          }
          .foreach {
            case (-1, node)    => all_nodes += node
            case (index, node) => all_nodes.update(index, node)
          }
        all_nodes --= all_nodes.filter(n => _nodesToDelete.contains(n.id))
        this._relationshipsBuffer.toArray
          .sortBy(_._1.value)
          .map {
            case (id, rel) => (index_relationships.indexOf(id.value), rel)
          }
          .foreach {
            case (-1, rel)    => all_rels += rel
            case (index, rel) => all_rels.update(index, rel)
          }
        all_rels --= all_rels.filter(r => _relationshipsToDelete.contains(r.id))
        _nodesBuffer.clear()
        _nodesToDelete.clear()
        _relationshipsBuffer.clear()
        _relationshipsToDelete.clear()
        true
      }
    }

    override def write: WriteTask = _writeTask

    override def indexManager: IndexManager = new IndexManager {
      override def createIndex(index: Index): Unit = _index += index

      override def dropIndex(index: Index): Unit = _index -= index

      override def indexes: Array[Index] = _index.toArray
    }

    override def statistics: Statistics = new Statistics {
      override def numNode: Long = nodes().length

      override def numNodeByLabel(labelName: LynxNodeLabel): Long =
        all_nodes.count(_.labels.contains(labelName))

      override def numNodeByProperty(
          labelName: LynxNodeLabel,
          propertyName: LynxPropertyKey,
          value: LynxValue
        ): Long =
        nodes(NodeFilter(Seq(labelName), Map(propertyName -> value))).length

      override def numRelationship: Long = relationships().length

      override def numRelationshipByType(typeName: LynxRelationshipType): Long =
        all_rels.count(_.relationType.forall(typeName.equals))
    }

    override def nodes(): Iterator[LynxNode] = all_nodes.iterator

    override def relationships(): Iterator[PathTriple] =
      all_rels.iterator.map(rel =>
        PathTriple(nodeAt(rel.startNodeId).get, rel, nodeAt(rel.endNodeId).get)
      )

  }

  def getOperatorAllOutputs(operator: ExecutionOperator): Array[RowBatch] = {
    val result = ArrayBuffer[RowBatch]()
    operator.open()
    var data = operator.getNext()
    while (data.batchData.nonEmpty) {
      result.append(data)
      data = operator.getNext()
    }
    operator.close()
    result.toArray
  }

  def getOperatorFlattenResult(operator: ExecutionOperator): List[LynxValue] = {
    getOperatorAllOutputs(operator).flatMap(batch => batch.batchData).flatten.toList
  }

  def prepareNodeScanOperator(
      schemaName: String,
      labelNames: Seq[String],
      properties: Seq[(PropertyKeyName, Expression)]
    ): NodeScanOperator = {
    val variable = Option(Variable(schemaName)(defaultPosition))
    val labels = labelNames.map(name => LabelName(name)(defaultPosition))
    val propertiesExpression = Option(
      MapExpression(
        properties
      )(defaultPosition)
    )
    val pattern = NodePattern(variable, labels, propertiesExpression)(defaultPosition)
    val operator = NodeScanOperator(pattern, model, expressionEvaluator, ctx.expressionContext)
    operator
  }
}
