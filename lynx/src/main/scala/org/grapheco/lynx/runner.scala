package org.grapheco.lynx

import com.typesafe.scalalogging.LazyLogging
import org.grapheco.lynx.graph.GraphModel
import org.grapheco.lynx.logical.LogicalNode
import org.grapheco.lynx.physical.PhysicalNode
import org.grapheco.lynx.planner.{DefaultLogicalPlanner, DefaultPhysicalPlanner, ExecutionContext, LogicalPlanner, LogicalPlannerContext, PhysicalPlanner, PhysicalPlannerContext, PlanAware}
import org.grapheco.lynx.procedure.functions.{AggregatingFunctions, ListFunctions, LogarithmicFunctions, NumericFunctions, PredicateFunctions, ScalarFunctions, StringFunctions, TimeFunctions, TrigonometricFunctions}
import org.grapheco.lynx.procedure.{DefaultProcedureRegistry, ProcedureRegistry}
import org.grapheco.lynx.util.FormatUtils
import org.grapheco.lynx.types.{DefaultTypeSystem, LynxResult, LynxValue, TypeSystem}
import org.opencypher.v9_0.ast.Statement
import org.opencypher.v9_0.ast.semantics.SemanticState
import org.grapheco.metrics.DomainObject

case class CypherRunnerContext(
    typeSystem: TypeSystem,
    procedureRegistry: ProcedureRegistry,
    dataFrameOperator: DataFrameOperator,
    expressionEvaluator: ExpressionEvaluator,
    graphModel: GraphModel)

class CypherRunner(graphModel: GraphModel) extends LazyLogging {
  protected lazy val types: TypeSystem = new DefaultTypeSystem()
  protected lazy val procedures: DefaultProcedureRegistry = new DefaultProcedureRegistry(
    types,
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
  protected lazy val expressionEvaluator: ExpressionEvaluator =
    new DefaultExpressionEvaluator(graphModel, types, procedures)
  protected lazy val dataFrameOperator: DataFrameOperator = new DefaultDataFrameOperator(
    expressionEvaluator
  )
  private implicit lazy val runnerContext =
    CypherRunnerContext(types, procedures, dataFrameOperator, expressionEvaluator, graphModel)
  protected lazy val logicalPlanner: LogicalPlanner = new DefaultLogicalPlanner(runnerContext)
  protected lazy val physicalPlanner: PhysicalPlanner = new DefaultPhysicalPlanner(runnerContext)
  protected lazy val physicalPlanOptimizer: PhysicalPlanOptimizer =
    new DefaultPhysicalPlanOptimizer(runnerContext)
  protected lazy val queryParser: QueryParser = new CachedQueryParser(
    new DefaultQueryParser(runnerContext)
  )

  def compile(query: String): (Statement, Map[String, Any], SemanticState) =
    queryParser.parse(query)

  def run(query: String, param: Map[String, Any]): LynxResult = {
    DomainObject.pushLabel(query)

    DomainObject.recordLatency(null)

    DomainObject.pushLabel("ast-plan")
    DomainObject.recordLatency(null)
    val (statement, param2, state) = queryParser.parse(query)
    logger.debug(s"AST tree: ${statement}")
    DomainObject.recordLatency(null)
    DomainObject.popLabel()

    val logicalPlannerContext = LogicalPlannerContext(param ++ param2, runnerContext)
    DomainObject.pushLabel("logical-plan")
    DomainObject.recordLatency(null)
    val logicalPlan = logicalPlanner.plan(statement, logicalPlannerContext)
    logger.debug(s"logical plan: \r\n${logicalPlan.pretty}")
    DomainObject.recordLatency(null)
    DomainObject.popLabel()

    val physicalPlannerContext = PhysicalPlannerContext(param ++ param2, runnerContext)
    DomainObject.pushLabel("physical-plan")
    DomainObject.recordLatency(null)
    val physicalPlan = physicalPlanner.plan(logicalPlan)(physicalPlannerContext)
    logger.debug(s"physical plan: \r\n${physicalPlan.pretty}")
    DomainObject.recordLatency(null)
    DomainObject.popLabel()

    DomainObject.pushLabel("optimizer")
    DomainObject.recordLatency(null)
    val optimizedPhysicalPlan = physicalPlanOptimizer.optimize(physicalPlan, physicalPlannerContext)
    logger.debug(s"optimized physical plan: \r\n${optimizedPhysicalPlan.pretty}")
    DomainObject.recordLatency(null)
    DomainObject.popLabel()

    val ctx = ExecutionContext(physicalPlannerContext, statement, param ++ param2)
    DomainObject.pushLabel("execute")
    DomainObject.recordLatency(null)
    val df = optimizedPhysicalPlan.execute(ctx)
    graphModel.write.commit
    DomainObject.recordLatency(null)
    DomainObject.popLabel()

    DomainObject.recordLatency(null)
    new LynxResult() with PlanAware {
      val schema = df.schema
      val columnNames = schema.map(_._1)

      override def show(limit: Int): Unit =
        FormatUtils.printTable(columnNames, df.records.take(limit).toSeq.map(_.map(_.value)))

      override def columns(): Seq[String] = columnNames

      override def records(): Iterator[Map[String, LynxValue]] =
        df.records.map(columnNames.zip(_).toMap)

      override def getASTStatement(): (Statement, Map[String, Any]) = (statement, param2)

      override def getLogicalPlan(): LogicalNode = logicalPlan

      override def getPhysicalPlan(): PhysicalNode = physicalPlan

      override def getOptimizerPlan(): PhysicalNode = optimizedPhysicalPlan

      override def cache(): LynxResult = {
        val source = this
        val cached = df.records.toSeq

        new LynxResult {
          override def show(limit: Int): Unit =
            FormatUtils.printTable(columnNames, cached.take(limit).toSeq.map(_.map(_.value)))

          override def cache(): LynxResult = this

          override def columns(): Seq[String] = columnNames

          override def records(): Iterator[Map[String, LynxValue]] =
            cached.map(columnNames.zip(_).toMap).iterator

        }
      }
    }
  }
}
