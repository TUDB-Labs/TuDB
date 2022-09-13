package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch}
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
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
  private var columnNames: Seq[String] = _
  private var columnTypeByName: Map[String, LynxType] = _
  private var columnIndexByName: Map[String, Int] = _

  override def openImpl(): Unit = {
    in.open()
    columnNames = in.outputSchema().map(f => f._1)
    columnTypeByName = in.outputSchema().toMap
    columnIndexByName = columnNames.zipWithIndex.toMap
  }

  override def getNextImpl(): RowBatch = {
    val batchData = in.getNext().batchData
    if (batchData.isEmpty) return RowBatch(Seq.empty)

    val updatedBatchData = batchData.map(record => {
      val variableValueByName = columnNames.zip(record).toMap
      var updatedRecord = record
      setItems.foreach {
        case SetPropertyItem(property, propertyValueExpr) => {
          val Property(entity, propKeyName) = property
          entity match {
            case Variable(entityVar) => {
              updatedRecord = setProperty(
                entityVar,
                propKeyName.name,
                propertyValueExpr,
                updatedRecord,
                variableValueByName
              )
            }
            case ce: CaseExpression => {
              updatedRecord = setPropertyByCaseExpr(
                ce,
                propKeyName.name,
                propertyValueExpr,
                updatedRecord,
                variableValueByName
              )
            }
            case unknown => {
              throw new TuDBException(
                TuDBError.UNKNOWN_ERROR,
                s"Not support expr: ${unknown.toString}"
              )
            }
          }
        }
        case SetLabelItem(variable, labels) => {
          updatedRecord = setLabel(
            variable.name,
            labels.map(l => l.name).toArray,
            updatedRecord,
            variableValueByName
          )
        }
        case SetIncludingPropertiesFromMapItem(variable, expression) => {
          expression match {
            case MapExpression(items) => {
              updatedRecord = setPropertiesFromMapExpr(
                variable.name,
                items,
                updatedRecord,
                variableValueByName,
                false
              )
            }
            case unknown => {
              throw new TuDBException(
                TuDBError.UNKNOWN_ERROR,
                s"Not support expr: ${unknown.toString}"
              )
            }
          }
        }
        case SetExactPropertiesFromMapItem(variable, expression) => {
          expression match {
            case MapExpression(items) => {
              updatedRecord = setPropertiesFromMapExpr(
                variable.name,
                items,
                updatedRecord,
                variableValueByName,
                true
              )
            }
            case Variable(maskName) => {
              updatedRecord = copyPropertiesFromNodeAndCleanExistProperties(
                variable.name,
                maskName,
                updatedRecord,
                variableValueByName
              )
            }
            case unknown => {
              throw new TuDBException(
                TuDBError.UNKNOWN_ERROR,
                s"Not support expr: ${unknown.toString}"
              )
            }
          }
        }
        case unknown => {
          throw new TuDBException(
            TuDBError.UNKNOWN_ERROR,
            s"Not support setItem: ${unknown.toString}"
          )
        }
      }
      updatedRecord
    })
    RowBatch(updatedBatchData)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()

  private def setLabel(
      entityVar: String,
      labelNames: Array[String],
      updatedRecord: Seq[LynxValue],
      variableValueByName: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    val nodeId = variableValueByName(entityVar).asInstanceOf[LynxNode].id
    val updatedNode = graphModel
      .setNodesLabels(Iterator(nodeId), labelNames)
      .next()
      .get
    updatedRecord.updated(columnIndexByName(entityVar), updatedNode)
  }
  private def copyPropertiesFromNodeAndCleanExistProperties(
      srcNodeName: String,
      maskNodeName: String,
      updatedRecord: Seq[LynxValue],
      variableValueByName: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    var srcNode = variableValueByName(srcNodeName).asInstanceOf[LynxNode]
    val maskNode = variableValueByName(maskNodeName).asInstanceOf[LynxNode]
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
    updatedRecord.updated(columnIndexByName(srcNodeName), srcNode)
  }

  private def setPropertiesFromMapExpr(
      entityVar: String,
      items: Seq[(PropertyKeyName, Expression)],
      updatedRecord: Seq[LynxValue],
      variableValueByName: Map[String, LynxValue],
      cleanExistProperties: Boolean
    ): Seq[LynxValue] = {
    val nodeId = variableValueByName(entityVar).asInstanceOf[LynxNode].id
    val props = items.map(item => {
      item._1.name -> expressionEvaluator
        .eval(item._2)(expressionContext.withVars(variableValueByName))
        .value
    })
    val updatedNode =
      graphModel
        .setNodesProperties(Iterator(nodeId), props.toArray, cleanExistProperties)
        .next()
        .get
    updatedRecord.updated(columnIndexByName(entityVar), updatedNode)
  }

  private def setProperty(
      entityVar: String,
      propKeyName: String,
      propertyValueExpr: Expression,
      updatedRecord: Seq[LynxValue],
      variableValueByName: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    if (!variableValueByName.contains(entityVar)) return updatedRecord

    val recordType = columnTypeByName(entityVar)
    val props = propKeyName -> expressionEvaluator
      .eval(propertyValueExpr)(expressionContext.withVars(variableValueByName))
      .value
    recordType match {
      case CTNode => {
        val nodeId = variableValueByName(entityVar).asInstanceOf[LynxNode].id
        val updatedNode =
          graphModel.setNodesProperties(Iterator(nodeId), Array(props)).next().get
        updatedRecord.updated(columnIndexByName(entityVar), updatedNode)
      }
      case CTRelationship => {
        val relId = variableValueByName(entityVar).asInstanceOf[LynxRelationship].id
        val updatedRel = graphModel
          .setRelationshipsProperties(Iterator(relId), Array(props))
          .next()
          .get
        updatedRecord.updated(columnIndexByName(entityVar), updatedRel)
      }
    }
  }

  private def setPropertyByCaseExpr(
      caseExpr: CaseExpression,
      propKeyName: String,
      propertyValueExpr: Expression,
      updatedRecord: Seq[LynxValue],
      variableValueByName: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    val caseResult = expressionEvaluator.eval(caseExpr)(
      expressionContext.withVars(variableValueByName)
    ) // node or null
    caseResult match {
      case LynxNull => updatedRecord
      case _ => {
        val variableValueByNameRevert = variableValueByName.map(kv => kv._2 -> kv._1)
        val updateSchemaName = variableValueByNameRevert(caseResult)

        val nodeId = caseResult.asInstanceOf[LynxNode].id
        val props = propKeyName -> expressionEvaluator
          .eval(propertyValueExpr)(expressionContext.withVars(variableValueByName))
          .value

        val updatedNode =
          graphModel.setNodesProperties(Iterator(nodeId), Array(props)).next().get
        updatedRecord.updated(columnIndexByName(updateSchemaName), updatedNode)
      }
    }
  }
}
