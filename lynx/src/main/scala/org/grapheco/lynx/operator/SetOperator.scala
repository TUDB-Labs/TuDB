package org.grapheco.lynx.operator

import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.lynx.{ExecutionOperator, ExpressionContext, ExpressionEvaluator, GraphModel, LynxType, RowBatch}
import org.opencypher.v9_0.ast.{SetExactPropertiesFromMapItem, SetIncludingPropertiesFromMapItem, SetItem, SetLabelItem, SetPropertyItem}
import org.opencypher.v9_0.expressions.{CaseExpression, MapExpression, Property, Variable}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

import scala.collection.mutable.ArrayBuffer

/**
  *@description: This operator is used to set properties to node or relationship and set label to node
  */
case class SetOperator(
    setItems: Seq[SetItem],
    in: ExecutionOperator,
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
    if (batchData.nonEmpty) {
      val updatedBatchData = batchData.map(record => {
        val ctxMap = schemaNames.zip(record).toMap
        var updatedRecord = record
        setItems.foreach {
          case SetPropertyItem(property, literalExpr) => {
            val Property(map, propKeyName) = property
            map match {
              case Variable(name) => {
                if (ctxMap.contains(name)) {
                  val recordType = schemaType(name)
                  val props = propKeyName.name -> expressionEvaluator
                    .eval(literalExpr)(expressionContext.withVars(ctxMap))
                    .value
                  recordType match {
                    case CTNode => {
                      val nodeId = ctxMap(name).asInstanceOf[LynxNode].id
                      val updatedNode =
                        graphModel.setNodesProperties(Iterator(nodeId), Array(props)).next().get
                      updatedRecord = updatedRecord.updated(schemaWithIndex(name), updatedNode)
                    }
                    case CTRelationship => {
                      val relId = ctxMap(name).asInstanceOf[LynxRelationship].id
                      val updatedRel = graphModel
                        .setRelationshipsProperties(Iterator(relId), Array(props))
                        .next()
                        .get
                      updatedRecord = updatedRecord.updated(schemaWithIndex(name), updatedRel)
                    }
                  }
                }
              }
              case ce: CaseExpression => {
                val caseResult = expressionEvaluator.eval(ce)(expressionContext.withVars(ctxMap)) // node or null
                caseResult match {
                  case LynxNull => {}
                  case _ => {
                    val ctxMapRevert = ctxMap.map(kv => kv._2 -> kv._1)
                    val updateSchemaName = ctxMapRevert(caseResult)

                    val nodeId = caseResult.asInstanceOf[LynxNode].id
                    val props = propKeyName.name -> expressionEvaluator
                      .eval(literalExpr)(expressionContext.withVars(ctxMap))
                      .value

                    val updatedNode =
                      graphModel.setNodesProperties(Iterator(nodeId), Array(props)).next().get
                    updatedRecord =
                      updatedRecord.updated(schemaWithIndex(updateSchemaName), updatedNode)
                  }
                }
              }
            }
          }
          case SetLabelItem(variable, labels) => {
            if (ctxMap.contains(variable.name)) {
              val nodeId = ctxMap(variable.name).asInstanceOf[LynxNode].id
              val updatedNode = graphModel
                .setNodesLabels(Iterator(nodeId), labels.map(l => l.name).toArray)
                .next()
                .get
              updatedRecord = updatedRecord.updated(schemaWithIndex(variable.name), updatedNode)
            }
          }
          case SetIncludingPropertiesFromMapItem(variable, expression) => {
            if (ctxMap.contains(variable.name)) {
              expression match {
                case MapExpression(items) => {
                  val nodeId = ctxMap(variable.name).asInstanceOf[LynxNode].id
                  val props = items.map(item => {
                    item._1.name -> expressionEvaluator
                      .eval(item._2)(expressionContext.withVars(ctxMap))
                      .value
                  })
                  val updatedNode =
                    graphModel.setNodesProperties(Iterator(nodeId), props.toArray).next().get
                  updatedRecord = updatedRecord.updated(schemaWithIndex(variable.name), updatedNode)
                }
              }
            }
          }
          case SetExactPropertiesFromMapItem(variable, expression) => {
            if (ctxMap.contains(variable.name)) {
              expression match {
                case MapExpression(items) => {
                  val nodeId = ctxMap(variable.name).asInstanceOf[LynxNode].id
                  val props = items.map(item => {
                    item._1.name -> expressionEvaluator
                      .eval(item._2)(expressionContext.withVars(ctxMap))
                      .value
                  })
                  val updatedNode =
                    graphModel.setNodesProperties(Iterator(nodeId), props.toArray, true).next().get
                  updatedRecord = updatedRecord.updated(schemaWithIndex(variable.name), updatedNode)
                }
              }
            }
          }
          case SetExactPropertiesFromMapItem(variable, expression) => {
            if (ctxMap.contains(variable.name)) {
              expression match {
                case Variable(name2) => {
                  var srcNode = ctxMap(variable.name).asInstanceOf[LynxNode]
                  val maskNode = ctxMap(name2).asInstanceOf[LynxNode]
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
                  updatedRecord = updatedRecord.updated(schemaWithIndex(variable.name), srcNode)
                }
              }
            }
          }
        }
        updatedRecord
      })
      RowBatch(updatedBatchData)
    } else RowBatch(Seq.empty)
  }

  override def closeImpl(): Unit = {}

  override def outputSchema(): Seq[(String, LynxType)] = in.outputSchema()

}
