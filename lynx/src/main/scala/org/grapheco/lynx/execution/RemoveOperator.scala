package org.grapheco.lynx.execution

import org.grapheco.lynx.graph.GraphModel
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, LynxType, RowBatch}
import org.opencypher.v9_0.ast.{RemoveItem, RemoveLabelItem, RemovePropertyItem}
import org.opencypher.v9_0.expressions.{Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

/**
  *@description: This operator is used to:
  *                1. remove properties from nodes and relationships
  *                2. remove labels from node.
  */
case class RemoveOperator(
    in: ExecutionOperator,
    removeItems: Seq[RemoveItem],
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val children: Seq[ExecutionOperator] = Seq(in)

  private var schemaNames: Seq[String] = _
  private var schemaTypes: Map[String, LynxType] = _
  private var schemaWithIndex: Map[String, Int] = _

  override def openImpl(): Unit = {
    in.open()
    schemaNames = in.outputSchema().map(f => f._1)
    schemaTypes = in.outputSchema().toMap
    schemaWithIndex = schemaNames.zipWithIndex.toMap
  }

  override def getNextImpl(): RowBatch = {
    val batchData = in.getNext().batchData
    if (batchData.isEmpty) return RowBatch(Seq.empty)
    val updatedBatchData = batchData.map(record => {
      val ctxMap = schemaNames.zip(record).toMap
      var updatedRecord = record
      removeItems.foreach {
        case RemovePropertyItem(property) => {
          property match {
            case Property(Variable(name), PropertyKeyName(keyName)) => {
              if (ctxMap.contains(name)) {
                val schemaType = schemaTypes(name)
                schemaType match {
                  case CTNode => {
                    val nodeId = ctxMap(name).asInstanceOf[LynxNode].id
                    val updatedNode = graphModel
                      .removeNodesProperties(Iterator(nodeId), Array(keyName))
                      .next()
                      .get
                    updatedRecord = updatedRecord.updated(schemaWithIndex(name), updatedNode)
                  }
                  case CTRelationship => {
                    val relId = ctxMap(name).asInstanceOf[LynxRelationship].id
                    val updatedNode = graphModel
                      .removeRelationshipsProperties(Iterator(relId), Array(keyName))
                      .next()
                      .get
                    updatedRecord = updatedRecord.updated(schemaWithIndex(name), updatedNode)
                  }
                  case _ => {}
                }
              }
            }
            case _ => {}
          }
        }
        case RemoveLabelItem(variable, labels) => {
          if (ctxMap.contains(variable.name)) {
            val nodeId = ctxMap(variable.name).asInstanceOf[LynxNode].id
            val updatedNode = graphModel
              .removeNodesLabels(Iterator(nodeId), labels.map(f => f.name).toArray)
              .next()
              .get
            updatedRecord = updatedRecord.updated(schemaWithIndex(variable.name), updatedNode)
          }
        }
      }
      updatedRecord
    })
    RowBatch(updatedBatchData)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()
}
