package org.grapheco.lynx.operator

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxMap
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
      val variableNameByValue =
        variableValueByName.map(nameAndValue => (nameAndValue._2, nameAndValue._1))
      var updatedRecord = record
      setItems.foreach {
        case SetPropertyItem(property, propertyValueExpr) => {
          val Property(entity, propKeyName) = property
          val lynxValue =
            expressionEvaluator.eval(entity)(expressionContext.withVars(variableValueByName))

          lynxValue match {
            case LynxNull => {}
            case _ => {
              val prop = expressionEvaluator
                .eval(propertyValueExpr)(expressionContext.withVars(variableValueByName))
                .value
              updatedRecord = setProperty(
                variableNameByValue(lynxValue),
                Map(propKeyName.name -> prop),
                updatedRecord,
                variableValueByName,
                false
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
          val lynxValue =
            expressionEvaluator.eval(expression)(expressionContext.withVars(variableValueByName))
          lynxValue match {
            case map: LynxMap => {
              val props = map.value.map(nameAndValue => (nameAndValue._1, nameAndValue._2.value))
              updatedRecord =
                setProperty(variable.name, props, updatedRecord, variableValueByName, false)
            }
            case LynxNull => {}
            case unknown => {
              throw new TuDBException(
                TuDBError.UNKNOWN_ERROR,
                s"Not support expr: ${unknown.toString}"
              )
            }
          }
        }
        case SetExactPropertiesFromMapItem(variable, expression) => {
          val lynxValue =
            expressionEvaluator.eval(expression)(expressionContext.withVars(variableValueByName))
          lynxValue match {
            case maskNode: LynxNode =>
              updatedRecord = copyPropertiesFromNodeAndCleanExistProperties(
                variable.name,
                maskNode,
                updatedRecord,
                variableValueByName
              )
            case map: LynxMap => {
              val props = map.value.map(nameAndValue => (nameAndValue._1, nameAndValue._2.value))
              updatedRecord =
                setProperty(variable.name, props, updatedRecord, variableValueByName, true)
            }
            case LynxNull => {}
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
      maskNode: LynxNode,
      updatedRecord: Seq[LynxValue],
      variableValueByName: Map[String, LynxValue]
    ): Seq[LynxValue] = {
    var srcNode = variableValueByName(srcNodeName).asInstanceOf[LynxNode]
    val maskNodeProps =
      maskNode.keys.map(k => k.value -> maskNode.property(k).get.value)

    srcNode = graphModel
      .setNodesProperties(Iterator(srcNode.id), maskNodeProps.toArray, true)
      .next()
      .get
    updatedRecord.updated(columnIndexByName(srcNodeName), srcNode)
  }

  private def setProperty(
      entityVar: String,
      props: Map[String, Any],
      updatedRecord: Seq[LynxValue],
      variableValueByName: Map[String, LynxValue],
      cleanExistProperties: Boolean
    ): Seq[LynxValue] = {
    val recordType = columnTypeByName(entityVar)
    recordType match {
      case CTNode => {
        val nodeId = variableValueByName(entityVar).asInstanceOf[LynxNode].id
        val updatedNode =
          graphModel
            .setNodesProperties(Iterator(nodeId), props.toArray, cleanExistProperties)
            .next()
            .get
        updatedRecord.updated(columnIndexByName(entityVar), updatedNode)
      }
      case CTRelationship => {
        val relId = variableValueByName(entityVar).asInstanceOf[LynxRelationship].id
        val updatedRel = graphModel
          .setRelationshipsProperties(Iterator(relId), props.toArray)
          .next()
          .get
        updatedRecord.updated(columnIndexByName(entityVar), updatedRel)
      }
    }
  }
}
