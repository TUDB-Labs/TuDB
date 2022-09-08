package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch}
import org.opencypher.v9_0.ast.{SetExactPropertiesFromMapItem, SetIncludingPropertiesFromMapItem, SetItem, SetLabelItem, SetPropertyItem}
import org.opencypher.v9_0.expressions.{CaseExpression, Expression, MapExpression, Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

/**
  *@description: This operator is used to:
  *               1. update labels on nodes.
  *               2. update properties on nodes and relationships.
  */
case class SetOperator(
    in: ExecutionOperator,
    setItems: Seq[SetItem],
    graphModel: GraphModel,
    expressionEvaluator: ExpressionEvaluator,
    expressionContext: ExpressionContext)
  extends ExecutionOperator {
  override val exprEvaluator: ExpressionEvaluator = expressionEvaluator
  override val exprContext: ExpressionContext = expressionContext
  private var schemaNames: Seq[String] = _
  private var schemaType: Map[String, LynxType] = _
  private var schemaWithIndex: Map[String, Int] = _

  override def openImpl(): Unit = {
    in.open()
    schemaNames = in.outputSchema().map(f => f._1)
    schemaType = in.outputSchema().toMap
    schemaWithIndex = schemaNames.zipWithIndex.toMap
  }

  override def getNextImpl(): RowBatch = {
    val batchData = in.getNext().batchData
    if (batchData.isEmpty) return RowBatch(Seq.empty)

    val updatedBatchData = batchData.map(record => {
      val ctxMap = schemaNames.zip(record).toMap
      var updatedRecord = record
      setItems.foreach {
        case SetPropertyItem(property, literalExpr) => {
          val Property(map, propKeyName) = property
          map match {
            case Variable(variableName) => {
              updatedRecord = setPropertyItemByVariable(
                variableName,
                propKeyName.name,
                literalExpr,
                updatedRecord,
                ctxMap
              )
            }
            case ce: CaseExpression => {
              updatedRecord =
                setPropertyItemByCaseExpr(ce, propKeyName.name, literalExpr, updatedRecord, ctxMap)
            }
          }
        }
        case SetLabelItem(variable, labels) => {
          if (ctxMap.contains(variable.name)) {
            updatedRecord =
              setLabelItem(variable.name, labels.map(l => l.name).toArray, updatedRecord, ctxMap)
          }
        }
        case SetIncludingPropertiesFromMapItem(variable, expression) => {
          if (ctxMap.contains(variable.name)) {
            expression match {
              case MapExpression(items) => {
                updatedRecord =
                  setPropertiesFromMapExpr(variable.name, items, updatedRecord, ctxMap, false)
              }
            }
          }
        }
        case SetExactPropertiesFromMapItem(variable, expression) => {
          if (ctxMap.contains(variable.name)) {
            expression match {
              case MapExpression(items) => {
                updatedRecord =
                  setPropertiesFromMapExpr(variable.name, items, updatedRecord, ctxMap, true)
              }
              case Variable(maskName) => {
                updatedRecord =
                  setExactPropertiesFromNode(variable.name, maskName, updatedRecord, ctxMap)
              }
            }
          }
        }
      }
      updatedRecord
    })
    RowBatch(updatedBatchData)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()

  private def setLabelItem(
      variableName: String,
      labelNames: Array[String],
      updatedRecord: Seq[LynxValue],
      ctxMap: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    val nodeId = ctxMap(variableName).asInstanceOf[LynxNode].id
    val updatedNode = graphModel
      .setNodesLabels(Iterator(nodeId), labelNames)
      .next()
      .get
    updatedRecord.updated(schemaWithIndex(variableName), updatedNode)
  }
  private def setExactPropertiesFromNode(
      srcNodeName: String,
      maskNodeName: String,
      updatedRecord: Seq[LynxValue],
      ctxMap: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    var srcNode = ctxMap(srcNodeName).asInstanceOf[LynxNode]
    val maskNode = ctxMap(maskNodeName).asInstanceOf[LynxNode]
    val maskNodeProps =
      maskNode.keys.map(k => k.value -> maskNode.property(k).get.value)

    srcNode = graphModel
      .setNodesLabels(Iterator(srcNode.id), maskNode.labels.map(l => l.value).toArray)
      .next()
      .get
    srcNode = graphModel
      .setNodesProperties(Iterator(srcNode.id), maskNodeProps.toArray, true)
      .next()
      .get
    updatedRecord.updated(schemaWithIndex(srcNodeName), srcNode)
  }

  private def setPropertiesFromMapExpr(
      variableName: String,
      items: Seq[(PropertyKeyName, Expression)],
      updatedRecord: Seq[LynxValue],
      ctxMap: Map[String, LynxValue],
      cleanExistProperties: Boolean
    ): Seq[LynxValue] = {
    val nodeId = ctxMap(variableName).asInstanceOf[LynxNode].id
    val props = items.map(item => {
      item._1.name -> expressionEvaluator
        .eval(item._2)(expressionContext.withVars(ctxMap))
        .value
    })
    val updatedNode =
      graphModel
        .setNodesProperties(Iterator(nodeId), props.toArray, cleanExistProperties)
        .next()
        .get
    updatedRecord.updated(schemaWithIndex(variableName), updatedNode)
  }

  private def setPropertyItemByVariable(
      variableName: String,
      propKeyName: String,
      literalExpr: Expression,
      updatedRecord: Seq[LynxValue],
      ctxMap: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    if (!ctxMap.contains(variableName)) return updatedRecord

    val recordType = schemaType(variableName)
    val props = propKeyName -> expressionEvaluator
      .eval(literalExpr)(expressionContext.withVars(ctxMap))
      .value
    recordType match {
      case CTNode => {
        val nodeId = ctxMap(variableName).asInstanceOf[LynxNode].id
        val updatedNode =
          graphModel.setNodesProperties(Iterator(nodeId), Array(props)).next().get
        updatedRecord.updated(schemaWithIndex(variableName), updatedNode)
      }
      case CTRelationship => {
        val relId = ctxMap(variableName).asInstanceOf[LynxRelationship].id
        val updatedRel = graphModel
          .setRelationshipsProperties(Iterator(relId), Array(props))
          .next()
          .get
        updatedRecord.updated(schemaWithIndex(variableName), updatedRel)
      }
    }
  }

  private def setPropertyItemByCaseExpr(
      caseExpr: CaseExpression,
      propKeyName: String,
      literalExpr: Expression,
      updatedRecord: Seq[LynxValue],
      ctxMap: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    val caseResult = expressionEvaluator.eval(caseExpr)(expressionContext.withVars(ctxMap)) // node or null
    caseResult match {
      case LynxNull => updatedRecord
      case _ => {
        val ctxMapRevert = ctxMap.map(kv => kv._2 -> kv._1)
        val updateSchemaName = ctxMapRevert(caseResult)

        val nodeId = caseResult.asInstanceOf[LynxNode].id
        val props = propKeyName -> expressionEvaluator
          .eval(literalExpr)(expressionContext.withVars(ctxMap))
          .value

        val updatedNode =
          graphModel.setNodesProperties(Iterator(nodeId), Array(props)).next().get
        updatedRecord.updated(schemaWithIndex(updateSchemaName), updatedNode)
      }
    }
  }
}
