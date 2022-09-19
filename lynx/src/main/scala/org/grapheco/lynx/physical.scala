package org.grapheco.lynx

import org.grapheco.lynx
import org.grapheco.lynx.physical.PhysicalExpandPath
import org.grapheco.lynx.procedure.CallableProcedure
import org.grapheco.lynx.procedure.exceptions.{UnknownProcedureException, WrongArgumentException, WrongNumberOfArgumentsException}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxNull, LynxPath}
import org.grapheco.lynx.types.structural.{LynxId, LynxNode, LynxNodeLabel, LynxPropertyKey, LynxRelationship, LynxRelationshipType}
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipChain, _}
import org.opencypher.v9_0.util.symbols.{CTAny, CTList, CTNode, CTPath, CTRelationship, CypherType, ListType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

trait PhysicalNode extends TreeNode {
  override type SerialType = PhysicalNode
  override val children: Seq[PhysicalNode] = Seq.empty
  val schema: Seq[(String, LynxType)]

  def execute(implicit ctx: ExecutionContext): DataFrame

  def withChildren(children0: Seq[PhysicalNode]): PhysicalNode
}

trait PhysicalNodeTranslator {
  def translate(in: Option[PhysicalNode])(implicit ppc: PhysicalPlannerContext): PhysicalNode
}

trait AbstractPhysicalNode extends PhysicalNode {

  val plannerContext: PhysicalPlannerContext

  implicit def ops(ds: DataFrame): DataFrameOps =
    DataFrameOps(ds)(plannerContext.runnerContext.dataFrameOperator)

  val typeSystem = plannerContext.runnerContext.typeSystem
  val graphModel = plannerContext.runnerContext.graphModel
  val expressionEvaluator = plannerContext.runnerContext.expressionEvaluator
  val procedureRegistry = plannerContext.runnerContext.procedureRegistry

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue =
    expressionEvaluator.eval(expr)

  def typeOf(expr: Expression): LynxType =
    plannerContext.runnerContext.expressionEvaluator
      .typeOf(expr, plannerContext.parameterTypes.toMap)

  def typeOf(expr: Expression, definedVarTypes: Map[String, LynxType]): LynxType =
    expressionEvaluator.typeOf(expr, definedVarTypes)

  def createUnitDataFrame(items: Seq[ReturnItem])(implicit ctx: ExecutionContext): DataFrame = {
    DataFrame.unit(items.map(item => item.name -> item.expression))(
      expressionEvaluator,
      ctx.expressionContext
    )
  }
}

trait PhysicalPlanner {
  def plan(logicalPlan: LogicalNode)(implicit plannerContext: PhysicalPlannerContext): PhysicalNode
}

class DefaultPhysicalPlanner(runnerContext: CypherRunnerContext) extends PhysicalPlanner {
  override def plan(
      logicalPlan: LogicalNode
    )(implicit plannerContext: PhysicalPlannerContext
    ): PhysicalNode = {
    implicit val runnerContext: CypherRunnerContext = plannerContext.runnerContext
    logicalPlan match {
      case LogicalProcedureCall(
          procedureNamespace: Namespace,
          procedureName: ProcedureName,
          declaredArguments: Option[Seq[Expression]]
          ) =>
        PhysicalProcedureCall(
          procedureNamespace: Namespace,
          procedureName: ProcedureName,
          declaredArguments: Option[Seq[Expression]]
        )
      case lc @ LogicalCreate(c: Create) =>
        PhysicalCreateTranslator(c).translate(lc.in.map(plan(_)))(plannerContext)
      case lm @ LogicalMerge(m: Merge) =>
        PhysicalMergeTranslator(m).translate(lm.in.map(plan(_)))(plannerContext)
      case lm @ LogicalMergeAction(m: Seq[MergeAction]) =>
        PhysicalMergeAction(m)(plan(lm.in.get), plannerContext)
      case ld @ LogicalDelete(d: Delete) => PhysicalDelete(d)(plan(ld.in), plannerContext)
      case ls @ LogicalSelect(columns: Seq[(String, Option[String])]) =>
        PhysicalSelect(columns)(plan(ls.in), plannerContext)
      case lp @ LogicalProject(ri)       => PhysicalProject(ri)(plan(lp.in), plannerContext)
      case la @ LogicalAggregation(a, g) => PhysicalAggregation(a, g)(plan(la.in), plannerContext)
      case lc @ LogicalCreateUnit(items) => PhysicalCreateUnit(items)(plannerContext)
      case lf @ LogicalFilter(expr)      => PhysicalFilter(expr)(plan(lf.in), plannerContext)
      case ld @ LogicalDistinct()        => PhysicalDistinct()(plan(ld.in), plannerContext)
      case ll @ LogicalLimit(expr)       => PhysicalLimit(expr)(plan(ll.in), plannerContext)
      case lo @ LogicalOrderBy(sortItem) => PhysicalOrderBy(sortItem)(plan(lo.in), plannerContext)
      case ll @ LogicalSkip(expr)        => PhysicalSkip(expr)(plan(ll.in), plannerContext)
      case lj @ LogicalJoin(isSingleMatch) =>
        PhysicalJoin(Seq.empty, isSingleMatch)(plan(lj.a), plan(lj.b), plannerContext)
      case patternMatch: LogicalPatternMatch =>
        PhysicalPatternMatchTranslator(patternMatch)(plannerContext).translate(None)
      case li @ LogicalCreateIndex(labelName: LabelName, properties: List[PropertyKeyName]) =>
        PhysicalCreateIndex(labelName, properties)(plannerContext)
      case sc @ LogicalSetClause(d) =>
        PhysicalSetClauseTranslator(d.items).translate(sc.in.map(plan(_)))(plannerContext)
      case lr @ LogicalRemove(r) =>
        PhysicalRemoveTranslator(r.items).translate(lr.in.map(plan(_)))(plannerContext)
      case lu @ LogicalUnwind(u) =>
        PhysicalUnwindTranslator(u.expression, u.variable)
          .translate(lu.in.map(plan(_)))(plannerContext)
    }
  }
}

case class PhysicalPatternMatchTranslator(
    patternMatch: LogicalPatternMatch
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends PhysicalNodeTranslator {
  private def planPatternMatch(
      pm: LogicalPatternMatch
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    val LogicalPatternMatch(headNode: NodePattern, chain: Seq[(RelationshipPattern, NodePattern)]) =
      pm
    chain.toList match {
      //match (m)
      case Nil => PhysicalNodeScan(headNode)(ppc)
      //match (m)-[r]-(n)
      case List(Tuple2(rel, rightNode)) => PhysicalRelationshipScan(rel, headNode, rightNode)(ppc)
      //match (m)-[r]-(n)-...-[p]-(z)
      case _ =>
        val (lastRelationship, lastNode) = chain.last
        val dropped = chain.dropRight(1)
        val part = planPatternMatch(LogicalPatternMatch(headNode, dropped))(ppc)
        PhysicalExpandPath(lastRelationship, lastNode)(part, plannerContext)
    }
  }

  override def translate(
      in: Option[PhysicalNode]
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    planPatternMatch(patternMatch)(ppc)
  }
}

case class PhysicalJoin(
    filterExpr: Seq[Expression],
    val isSingleMatch: Boolean,
    bigTableIndex: Int = 1
  )(a: PhysicalNode,
    b: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(a, b)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df1 = a.execute(ctx)
    val df2 = b.execute(ctx)

    val df = df1.join(df2, isSingleMatch, bigTableIndex)

    // TODO: eval function each time can only process one expression,
    //  so if there are many filterExpression, we will filter DataFrame several times. can speed up?
    if (filterExpr.nonEmpty) {
      val ec = ctx.expressionContext
      var filteredDataFrame: DataFrame = DataFrame.empty
      filterExpr.foreach(expr => {
        filteredDataFrame = df.filter { (record: Seq[LynxValue]) =>
          eval(expr)(ec.withVars(df.schema.map(_._1).zip(record).toMap)) match {
            case LynxBoolean(b) => b
            case LynxNull       => false
          }
        }(ec)
      })
      filteredDataFrame
    } else df
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalJoin =
    PhysicalJoin(filterExpr, isSingleMatch)(children0.head, children0(1), plannerContext)

  override val schema: Seq[(String, LynxType)] = (a.schema ++ b.schema).distinct
}

case class PhysicalDistinct()(implicit in: PhysicalNode, val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.distinct()
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalDistinct =
    PhysicalDistinct()(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema
}

case class PhysicalOrderBy(
    sortItem: Seq[SortItem]
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val schema: Seq[(String, LynxType)] = in.schema
  override val children: Seq[PhysicalNode] = Seq(in)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    implicit val ec: ExpressionContext = ctx.expressionContext
    /*    val sortItems:Seq[(String ,Boolean)] = sortItem.map {
          case AscSortItem(expression) => (expression.asInstanceOf[Variable].name, true)
          case DescSortItem(expression) => (expression.asInstanceOf[Variable].name, false)
        }*/
    val sortItems2: Seq[(Expression, Boolean)] = sortItem.map {
      case AscSortItem(expression)  => (expression, true)
      case DescSortItem(expression) => (expression, false)
    }
    df.orderBy(sortItems2)(ec)
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalNode =
    PhysicalOrderBy(sortItem)(children0.head, plannerContext)
}

case class PhysicalLimit(
    expr: Expression
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    implicit val ec = ctx.expressionContext
    df.take(eval(expr).value.asInstanceOf[Number].intValue())
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalLimit =
    PhysicalLimit(expr)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema
}

case class PhysicalSkip(
    expr: Expression
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    implicit val ec = ctx.expressionContext
    df.skip(eval(expr).value.asInstanceOf[Number].intValue())
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalSkip =
    PhysicalSkip(expr)(children0.head, plannerContext)
}

case class PhysicalFilter(
    expr: Expression
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val ec = ctx.expressionContext
    df.filter { (record: Seq[LynxValue]) =>
      eval(expr)(ec.withVars(df.schema.map(_._1).zip(record).toMap)) match {
        case LynxBoolean(b) => b
        case LynxNull       => false //todo check logic
      }
    }(ec)
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalFilter =
    PhysicalFilter(expr)(children0.head, plannerContext)
}

case class PhysicalNodeScan(
    pattern: NodePattern
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalNodeScan =
    PhysicalNodeScan(pattern)(plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val NodePattern(
      Some(var0: LogicalVariable),
      labels: Seq[LabelName],
      properties: Option[Expression],
      baseNode: Option[LogicalVariable]
    ) = pattern
    Seq(var0.name -> CTNode)
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val NodePattern(
      Some(var0: LogicalVariable),
      labels: Seq[LabelName],
      properties: Option[Expression],
      baseNode: Option[LogicalVariable]
    ) = pattern
    implicit val ec = ctx.expressionContext

    DataFrame(
      Seq(var0.name -> CTNode),
      () => {
        val nodes = if (labels.isEmpty) {
          graphModel.nodes(
            NodeFilter(
              Seq.empty,
              properties
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            )
          )
        } else
          graphModel.nodes(
            NodeFilter(
              labels.map(_.name).map(LynxNodeLabel),
              properties
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            )
          )

        nodes.map(Seq(_))
      }
    )
  }
}

case class PhysicalRelationshipScan(
    rel: RelationshipPattern,
    leftNode: NodePattern,
    rightNode: NodePattern
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalRelationshipScan =
    PhysicalRelationshipScan(rel, leftNode, rightNode)(plannerContext)

  override val schema: Seq[(String, LynxType)] = {
    val RelationshipPattern(
      var2: Option[LogicalVariable],
      types: Seq[RelTypeName],
      length: Option[Option[Range]],
      props2: Option[Expression],
      direction: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]
    ) = rel
    val NodePattern(
      var1,
      labels1: Seq[LabelName],
      props1: Option[Expression],
      baseNode1: Option[LogicalVariable]
    ) = leftNode
    val NodePattern(
      var3,
      labels3: Seq[LabelName],
      props3: Option[Expression],
      baseNode3: Option[LogicalVariable]
    ) = rightNode

    if (length.isEmpty) {
      Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
      )
    } else {
      Seq(
        var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
        var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(
          CTRelationship
        ),
        var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
      )
    }
  }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val RelationshipPattern(
      var2: Option[LogicalVariable],
      types: Seq[RelTypeName],
      length: Option[Option[Range]],
      props2: Option[Expression],
      direction: SemanticDirection,
      legacyTypeSeparator: Boolean,
      baseRel: Option[LogicalVariable]
    ) = rel
    val NodePattern(
      var1,
      labels1: Seq[LabelName],
      props1: Option[Expression],
      baseNode1: Option[LogicalVariable]
    ) = leftNode
    val NodePattern(
      var3,
      labels3: Seq[LabelName],
      props3: Option[Expression],
      baseNode3: Option[LogicalVariable]
    ) = rightNode

    implicit val ec = ctx.expressionContext

    val schema = {
      if (length.isEmpty) {
        Seq(
          var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
          var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rel.hashCode}") -> CTRelationship,
          var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      } else {
        Seq(
          var1.map(_.name).getOrElse(s"__NODE_${leftNode.hashCode}") -> CTNode,
          var2.map(_.name).getOrElse(s"__RELATIONSHIP_LIST_${rel.hashCode}") -> CTList(
            CTRelationship
          ),
          var3.map(_.name).getOrElse(s"__NODE_${rightNode.hashCode}") -> CTNode
        )
      }
    }

    //    length:
    //      [r:XXX] = None
    //      [r:XXX*] = Some(None) // degree 1 to MAX
    //      [r:XXX*..] =Some(Some(Range(None, None))) // degree 1 to MAX
    //      [r:XXX*..3] = Some(Some(Range(None, 3)))
    //      [r:XXX*1..] = Some(Some(Range(1, None)))
    //      [r:XXX*1..3] = Some(Some(Range(1, 3)))
    val (lowerLimit, upperLimit) = length match {
      case None       => (1, 1)
      case Some(None) => (1, Int.MaxValue)
      case Some(Some(Range(a, b))) => {
        (a, b) match {
          case (_, None) => (a.get.value.toInt, Int.MaxValue)
          case (None, _) => (1, b.get.value.toInt)
          case _         => (a.get.value.toInt, b.get.value.toInt)
        }
      }
    }

    DataFrame(
      schema,
      () => {
        val data = graphModel
          .paths(
            NodeFilter(
              labels1.map(_.name).map(LynxNodeLabel),
              props1
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            ),
            RelationshipFilter(
              types.map(_.name).map(LynxRelationshipType),
              props2
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            ),
            NodeFilter(
              labels3.map(_.name).map(LynxNodeLabel),
              props3
                .map(eval(_).asInstanceOf[LynxMap].value.map(kv => (LynxPropertyKey(kv._1), kv._2)))
                .getOrElse(Map.empty)
            ),
            direction,
            Option(upperLimit),
            Option(lowerLimit)
          )
        val relCypherType = schema(1)._2
        relCypherType match {
          case r @ CTRelationship => {
            data.map(f => Seq(f.head.startNode, f.head.storedRelation, f.head.endNode))
          }
          // process relationship Path to support like (a)-[r:TYPE*1..3]->(b)
          case rs @ ListType(CTRelationship) => {
            data.map(f => Seq(f.head.startNode, LynxPath(f), f.last.endNode))
          }
        }
      }
    )
  }
}

case class PhysicalCreateUnit(items: Seq[ReturnItem])(val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalCreateUnit =
    PhysicalCreateUnit(items)(plannerContext)

  override val schema: Seq[(String, LynxType)] =
    items.map(item => item.name -> typeOf(item.expression))

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    createUnitDataFrame(items)
  }
}

case class PhysicalSelect(
    columns: Seq[(String, Option[String])]
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalSelect =
    PhysicalSelect(columns)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] =
    columns.map(x => x._2.getOrElse(x._1)).map(x => x -> in.schema.find(_._1 == x).get._2)

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.select(columns)
  }
}

case class PhysicalProject(
    ri: ReturnItemsDef
  )(implicit val in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalProject =
    PhysicalProject(ri)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] =
    ri.items.map(x => x.name -> x.expression).map { col =>
      col._1 -> typeOf(col._2, in.schema.toMap)
    }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.project(ri.items.map(x => x.name -> x.expression))(ctx.expressionContext)
  }

  def withReturnItems(items: Seq[ReturnItem]) =
    PhysicalProject(ReturnItems(ri.includeExisting, items)(ri.position))(in, plannerContext)
}

case class PhysicalAggregation(
    aggregations: Seq[ReturnItem],
    groupings: Seq[ReturnItem]
  )(implicit val in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalAggregation =
    PhysicalAggregation(aggregations, groupings)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] =
    (groupings ++ aggregations).map(x => x.name -> x.expression).map { col =>
      col._1 -> typeOf(col._2, in.schema.toMap)
    }

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    df.groupBy(
      groupings.map(x => x.name -> x.expression),
      aggregations.map(x => x.name -> x.expression)
    )(ctx.expressionContext)
  }
}

case class PhysicalProcedureCall(
    procedureNamespace: Namespace,
    procedureName: ProcedureName,
    declaredArguments: Option[Seq[Expression]]
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalProcedureCall =
    PhysicalProcedureCall(procedureNamespace, procedureName, declaredArguments)(plannerContext)

  val Namespace(parts: List[String]) = procedureNamespace
  val ProcedureName(name: String) = procedureName
  val arguments = declaredArguments.getOrElse(Seq.empty)
  val procedure = procedureRegistry
    .getProcedure(parts, name)
    .getOrElse { throw UnknownProcedureException(parts, name) }

  override val schema: Seq[(String, LynxType)] = procedure.outputs

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val args = declaredArguments match {
      case Some(args) => args.map(eval(_)(ctx.expressionContext))
      case None =>
        procedure.inputs.map(arg => ctx.expressionContext.params.getOrElse(arg._1, LynxNull))
    }
    val argsType = args.map(_.lynxType)
    if (procedure.checkArgumentsType(argsType)) {
      DataFrame(procedure.outputs, () => Iterator(Seq(procedure.call(args))))
    } else {
      throw WrongArgumentException(name, procedure.inputs.map(_._2), argsType)
    }
  }
}

case class PhysicalCreateIndex(
    labelName: LabelName,
    properties: List[PropertyKeyName]
  )(implicit val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    graphModel._helper.createIndex(labelName.name, properties.map(_.name).toSet)
    DataFrame.empty
  }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalNode = this

  override val schema: Seq[(String, LynxType)] = {
    Seq("CreateIndex" -> CTAny)
  }
}

///////////////////////merge/////////////
trait MergeElement {
  def getSchema: String
}

case class MergeNode(varName: String, labels: Seq[LabelName], properties: Option[Expression])
  extends MergeElement {
  override def getSchema: String = varName
}

case class MergeRelationship(
    varName: String,
    types: Seq[RelTypeName],
    properties: Option[Expression],
    varNameLeftNode: String,
    varNameRightNode: String,
    direction: SemanticDirection)
  extends MergeElement {
  override def getSchema: String = varName
}

case class PhysicalMergeAction(
    actions: Seq[MergeAction]
  )(implicit in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override def withChildren(children0: Seq[PhysicalNode]): PhysicalMergeAction =
    PhysicalMergeAction(actions)(children0.head, plannerContext)

  override val children: Seq[PhysicalNode] = Seq(in)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    PhysicalSetClause(Seq.empty, actions)(in, plannerContext).execute(ctx)
  }
}

case class PhysicalMergeTranslator(m: Merge) extends PhysicalNodeTranslator {
  def translate(
      in: Option[PhysicalNode]
    )(implicit plannerContext: PhysicalPlannerContext
    ): PhysicalNode = {
    val definedVars = in.map(_.schema.map(_._1)).getOrElse(Seq.empty).toSet
    val mergeOps = mutable.ArrayBuffer[MergeElement]()
    val mergeSchema = mutable.ArrayBuffer[(String, LynxType)]()

    m.pattern.patternParts.foreach {
      case EveryPath(element) => {
        element match {
          case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) => {
            val leftNodeName = var1.map(_.name).getOrElse(s"__NODE_${element.hashCode}")
            mergeSchema.append((leftNodeName, CTNode))
            mergeOps.append(MergeNode(leftNodeName, labels1, properties1))
          }
          case chain: RelationshipChain => {
            buildMerge(chain, definedVars, mergeSchema, mergeOps)
          }
        }
      }
    }

    PhysicalMerge(mergeSchema, mergeOps)(in, plannerContext)
  }

  private def buildMerge(
      chain: RelationshipChain,
      definedVars: Set[String],
      mergeSchema: mutable.ArrayBuffer[(String, LynxType)],
      mergeOps: mutable.ArrayBuffer[MergeElement]
    ): String = {
    val RelationshipChain(
      left,
      rp @ RelationshipPattern(
        var2: Option[LogicalVariable],
        types: Seq[RelTypeName],
        length: Option[Option[Range]],
        properties2: Option[Expression],
        direction: SemanticDirection,
        legacyTypeSeparator: Boolean,
        baseRel: Option[LogicalVariable]
      ),
      rnp @ NodePattern(var3, labels3: Seq[LabelName], properties3: Option[Expression], _)
    ) = chain

    val varRelation = var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rp.hashCode}")
    val varRightNode = var3.map(_.name).getOrElse(s"__NODE_${rnp.hashCode}")
    left match {
      //create (m)-[r]-(n), left=n
      case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
        val varLeftNode = var1.map(_.name).getOrElse(s"__NODE_${left.hashCode}")
        mergeSchema.append((varLeftNode, CTNode))
        mergeSchema.append((varRelation, CTRelationship))
        mergeSchema.append((varRightNode, CTNode))

        mergeOps.append(MergeNode(varLeftNode, labels1, properties1))
        mergeOps.append(
          MergeRelationship(varRelation, types, properties2, varLeftNode, varRightNode, direction)
        )
        mergeOps.append(MergeNode(varRightNode, labels3, properties3))

        varRightNode

      // (m)-[p]-(t)-[r]-(n), leftChain=(m)-[p]-(t)
      case leftChain: RelationshipChain =>
        // (m)-[p]-(t)
        val lastNode = buildMerge(leftChain, definedVars, mergeSchema, mergeOps)
        mergeSchema.append((varRelation, CTRelationship))
        mergeSchema.append((varRightNode, CTNode))

        mergeOps.append(
          MergeRelationship(varRelation, types, properties2, lastNode, varRightNode, direction)
        )
        mergeOps.append(MergeNode(varRightNode, labels3, properties3))

        varRightNode
    }

  }
}

// if pattern not exists, create all elements in mergeOps
case class PhysicalMerge(
    mergeSchema: Seq[(String, LynxType)],
    mergeOps: Seq[MergeElement]
  )(implicit val in: Option[PhysicalNode],
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = in.toSeq

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalMerge =
    PhysicalMerge(mergeSchema, mergeOps)(children0.headOption, plannerContext)

  override val schema: Seq[(String, LynxType)] = mergeSchema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext

    //    def createNode(nodesToCreate: Seq[MergeNode],
    //                   mergedNodesAndRels: mutable.Map[String, Seq[LynxValue]],
    //                   ctxMap: Map[String, LynxValue] = Map.empty,
    //                   forceToCreate: Boolean = false): Unit = {
    //      nodesToCreate.foreach(mergeNode => {
    //        val MergeNode(varName, labels, properties) = mergeNode
    //        val nodeFilter = NodeFilter(labels.map(f => f.name), properties.map {
    //          case MapExpression(items) =>
    //            items.map({
    //              case (k, v) => k.name -> eval(v)(ec.withVars(ctxMap))
    //            })
    //        }.getOrElse(Seq.empty).toMap)
    //
    //        val res = {
    //          if (ctxMap.contains(varName)) ctxMap(varName)
    //          else graphModel.mergeNode(nodeFilter, forceToCreate, ctx.tx)
    //        }
    //
    //        mergedNodesAndRels += varName -> Seq(res)
    //      })
    //    }
    //
    //    def createRelationship(relsToCreate: Seq[MergeRelationship], createdNodesAndRels: mutable.Map[String, Seq[LynxValue]], ctxMap: Map[String, LynxValue] = Map.empty, forceToCreate: Boolean = false): Unit = {
    //      relsToCreate.foreach(mergeRels => {
    //        val MergeRelationship(varName, types, properties, varNameLeftNode, varNameRightNode, direction) = mergeRels
    //        val relationshipFilter = RelationshipFilter(types.map(t => t.name),
    //          properties.map {
    //            case MapExpression(items) =>
    //              items.map({
    //                case (k, v) => k.name -> eval(v)(ec.withVars(ctxMap))
    //              })
    //          }.getOrElse(Seq.empty[(String, LynxValue)]).toMap)
    //
    //        val leftNode = {
    //          if (createdNodesAndRels.contains(varNameLeftNode)) createdNodesAndRels(varNameLeftNode).head
    //          else if (ctxMap.contains(varNameLeftNode)) ctxMap(varNameLeftNode)
    //          else ???
    //        }
    //        val rightNode = {
    //          if (createdNodesAndRels.contains(varNameRightNode)) createdNodesAndRels(varNameRightNode).head
    //          else if (ctxMap.contains(varNameRightNode)) ctxMap(varNameRightNode)
    //          else ???
    //        }
    //
    //        val res = graphModel.mergeRelationship(relationshipFilter, leftNode.asInstanceOf[LynxNode], rightNode.asInstanceOf[LynxNode], direction, forceToCreate, ctx.tx)
    //
    //        createdNodesAndRels += varName -> Seq(res.storedRelation)
    //      })
    //    }

    children match {
      case Seq(pj @ PhysicalJoin(filterExpr, isSingleMatch, bigTableIndex)) => {
        val searchVar = pj.children.head.schema.toMap
        val res = children.map(_.execute).head.records
        if (res.nonEmpty) {
          plannerContext.pptContext ++= Map("MergeAction" -> false)
          DataFrame(pj.schema, () => res)
        } else {
          plannerContext.pptContext ++= Map("MergeAction" -> true)

          val toCreateSchema = mergeSchema.filter(f => !searchVar.contains(f._1))
          val toCreateList = mergeOps.filter(f => !searchVar.contains(f.getSchema))
          val nodesToCreate =
            toCreateList.filter(f => f.isInstanceOf[MergeNode]).map(f => f.asInstanceOf[MergeNode])
          val relsToCreate = toCreateList
            .filter(f => f.isInstanceOf[MergeRelationship])
            .map(f => f.asInstanceOf[MergeRelationship])

          val dependDf = pj.children.head.execute
          val anotherDf = pj.children.last

          val func = dependDf.records.map { record =>
            {
              val ctxMap = dependDf.schema.zip(record).map(x => x._1._1 -> x._2).toMap

              val mergedNodesAndRels = mutable.Map[String, Seq[LynxValue]]()

              val forceToCreate = {
                if (anotherDf.isInstanceOf[PhysicalExpandPath] || anotherDf
                      .isInstanceOf[PhysicalRelationshipScan]) true
                else false
              }

              //              createNode(nodesToCreate, mergedNodesAndRels, ctxMap, forceToCreate)
              //              createRelationship(relsToCreate, mergedNodesAndRels, ctxMap, forceToCreate)

              record ++ toCreateSchema.flatMap(f => mergedNodesAndRels(f._1))
            }
          }
          DataFrame(dependDf.schema ++ toCreateSchema, () => func)
        }
      }
      case _ => {
        // only merge situation
        val res = children.map(_.execute).head.records
        if (res.nonEmpty) {
          plannerContext.pptContext ++= Map("MergeAction" -> false)
          DataFrame(mergeSchema, () => res)
        } else {
          plannerContext.pptContext ++= Map("MergeAction" -> true)
          val createdNodesAndRels = mutable.Map[String, Seq[LynxValue]]()
          val nodesToCreate =
            mergeOps.filter(f => f.isInstanceOf[MergeNode]).map(f => f.asInstanceOf[MergeNode])
          val relsToCreate = mergeOps
            .filter(f => f.isInstanceOf[MergeRelationship])
            .map(f => f.asInstanceOf[MergeRelationship])

          //          createNode(nodesToCreate, createdNodesAndRels, Map.empty, true)
          //          createRelationship(relsToCreate, createdNodesAndRels, Map.empty, true)

          val res = Seq(mergeSchema.flatMap(f => createdNodesAndRels(f._1))).toIterator
          DataFrame(mergeSchema, () => res)
        }
      }
    }
  }
}

/////////////////////////////////////////

trait CreateElement

case class CreateNode(varName: String, labels3: Seq[LabelName], properties3: Option[Expression])
  extends CreateElement

case class CreateRelationship(
    varName: String,
    types: Seq[RelTypeName],
    properties: Option[Expression],
    varNameFromNode: String,
    varNameToNode: String)
  extends CreateElement

case class PhysicalCreateTranslator(c: Create) extends PhysicalNodeTranslator {
  def translate(
      in: Option[PhysicalNode]
    )(implicit plannerContext: PhysicalPlannerContext
    ): PhysicalNode = {
    val definedVars = in.map(_.schema.map(_._1)).getOrElse(Seq.empty).toSet
    val (schemaLocal, ops) =
      c.pattern.patternParts.foldLeft((Seq.empty[(String, LynxType)], Seq.empty[CreateElement])) {
        (result, part) =>
          val (schema1, ops1) = result
          part match {
            case EveryPath(element) =>
              element match {
                //create (n)
                case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
                  val leftNodeName = var1.map(_.name).getOrElse(s"__NODE_${element.hashCode}")
                  (schema1 :+ (leftNodeName -> CTNode)) ->
                    (ops1 :+ CreateNode(leftNodeName, labels1, properties1))

                //create (m)-[r]-(n)
                case chain: RelationshipChain =>
                  val (_, schema2, ops2) = build(chain, definedVars)
                  (schema1 ++ schema2) -> (ops1 ++ ops2)
              }
          }
      }

    PhysicalCreate(schemaLocal, ops)(in, plannerContext)
  }

  //returns (varLastNode, schema, ops)
  private def build(
      chain: RelationshipChain,
      definedVars: Set[String]
    ): (String, Seq[(String, LynxType)], Seq[CreateElement]) = {
    val RelationshipChain(
      left,
      rp @ RelationshipPattern(
        var2: Option[LogicalVariable],
        types: Seq[RelTypeName],
        length: Option[Option[Range]],
        properties2: Option[Expression],
        direction: SemanticDirection,
        legacyTypeSeparator: Boolean,
        baseRel: Option[LogicalVariable]
      ),
      rnp @ NodePattern(var3, labels3: Seq[LabelName], properties3: Option[Expression], _)
    ) = chain

    val varRelation = var2.map(_.name).getOrElse(s"__RELATIONSHIP_${rp.hashCode}")
    val varRightNode = var3.map(_.name).getOrElse(s"__NODE_${rnp.hashCode}")

    val schemaLocal = ArrayBuffer[(String, LynxType)]()
    val opsLocal = ArrayBuffer[CreateElement]()
    left match {
      //create (m)-[r]-(n), left=n
      case NodePattern(var1: Option[LogicalVariable], labels1, properties1, _) =>
        val varLeftNode = var1.map(_.name).getOrElse(s"__NODE_${left.hashCode}")
        if (!definedVars.contains(varLeftNode)) {
          schemaLocal += varLeftNode -> CTNode
          opsLocal += CreateNode(varLeftNode, labels1, properties1)
        }

        if (!definedVars.contains(varRightNode)) {
          schemaLocal += varRightNode -> CTNode
          opsLocal += CreateNode(varRightNode, labels3, properties3)
        }

        schemaLocal += varRelation -> CTRelationship
        direction match {
          case SemanticDirection.OUTGOING =>
            opsLocal += CreateRelationship(
              varRelation,
              types,
              properties2,
              varLeftNode,
              varRightNode
            )
          case SemanticDirection.INCOMING =>
            opsLocal += CreateRelationship(
              varRelation,
              types,
              properties2,
              varRightNode,
              varLeftNode
            )
          case SemanticDirection.BOTH =>
            throw new TuDBException(
              TuDBError.LYNX_UNSUPPORTED_OPERATION,
              "Not allowed to create a both relationship"
            )
        }

        (varRightNode, schemaLocal, opsLocal)

      //create (m)-[p]-(t)-[r]-(n), leftChain=(m)-[p]-(t)
      case leftChain: RelationshipChain =>
        //build (m)-[p]-(t)
        val (varLastNode, schema, ops) = build(leftChain, definedVars)
        (
          varRightNode,
          schema ++ Seq(varRelation -> CTRelationship) ++ (if (!definedVars.contains(varRightNode)) {
                                                             Seq(varRightNode -> CTNode)
                                                           } else {
                                                             Seq.empty
                                                           }),
          ops ++ (if (!definedVars.contains(varRightNode)) {
                    Seq(CreateNode(varRightNode, labels3, properties3))
                  } else {
                    Seq.empty
                  }) ++ Seq(
            direction match {
              case SemanticDirection.OUTGOING =>
                CreateRelationship(
                  varRelation,
                  types,
                  properties2,
                  varLastNode,
                  varRightNode
                )
              case SemanticDirection.INCOMING =>
                CreateRelationship(
                  varRelation,
                  types,
                  properties2,
                  varRightNode,
                  varLastNode
                )
              case SemanticDirection.BOTH =>
                throw new TuDBException(
                  TuDBError.LYNX_UNSUPPORTED_OPERATION,
                  "Not allowed to create a both relationship"
                )
            }
          )
        )
    }
  }
}

case class PhysicalCreate(
    schemaLocal: Seq[(String, LynxType)],
    ops: Seq[CreateElement]
  )(implicit val in: Option[PhysicalNode],
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = in.toSeq

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalCreate =
    PhysicalCreate(schemaLocal, ops)(children0.headOption, plannerContext)

  override val schema: Seq[(String, LynxType)] =
    in.map(_.schema).getOrElse(Seq.empty) ++ schemaLocal

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    implicit val ec = ctx.expressionContext
    val df = in.map(_.execute(ctx)).getOrElse(createUnitDataFrame(Seq.empty))
    //DataFrame should be generated first
    DataFrame.cached(
      schema,
      df.records.map { record =>
        val ctxMap = df.schema.zip(record).map(x => x._1._1 -> x._2).toMap
        val nodesInput = ArrayBuffer[(String, NodeInput)]()
        val relsInput = ArrayBuffer[(String, RelationshipInput)]()

        ops.foreach(_ match {
          case CreateNode(
              varName: String,
              labels: Seq[LabelName],
              properties: Option[Expression]
              ) =>
            if (!ctxMap.contains(varName) && nodesInput.find(_._1 == varName).isEmpty) {
              nodesInput += varName -> NodeInput(
                labels.map(_.name).map(LynxNodeLabel),
                properties
                  .map {
                    case MapExpression(items) =>
                      items.map({
                        case (k, v) =>
                          LynxPropertyKey(k.name) -> eval(v)(ec.withVars(ctxMap))
                      })
                  }
                  .getOrElse(Seq.empty)
              )
            }

          case CreateRelationship(
              varName: String,
              types: Seq[RelTypeName],
              properties: Option[Expression],
              varNameFromNode: String,
              varNameToNode: String
              ) =>
            def nodeInputRef(varname: String): NodeInputRef = {
              ctxMap
                .get(varname)
                .map(x => StoredNodeInputRef(x.asInstanceOf[LynxNode].id))
                .getOrElse(
                  ContextualNodeInputRef(varname)
                )
            }

            relsInput += varName -> RelationshipInput(
              types.map(_.name).map(LynxRelationshipType),
              properties
                .map {
                  case MapExpression(items) =>
                    items.map({
                      case (k, v) =>
                        LynxPropertyKey(k.name) -> eval(v)(ec.withVars(ctxMap))
                    })
                }
                .getOrElse(Seq.empty[(LynxPropertyKey, LynxValue)]),
              nodeInputRef(varNameFromNode),
              nodeInputRef(varNameToNode)
            )
        })

        record ++ graphModel.createElements(
          nodesInput,
          relsInput,
          (nodesCreated: Seq[(String, LynxNode)], relsCreated: Seq[(String, LynxRelationship)]) => {
            val created = nodesCreated.toMap ++ relsCreated
            schemaLocal.map(x => created(x._1))
          }
        )
      }.toSeq
    )
  }
}

/** The DELETE clause is used to delete graph elements — nodes, relationships or paths.
  * @param delete
  * @param in
  * @param plannerContext
  */
case class PhysicalDelete(
    delete: Delete
  )(implicit val in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalDelete =
    PhysicalDelete(delete)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = Seq.empty

  override def execute(implicit ctx: ExecutionContext): DataFrame = { // TODO so many bugs !
    val df = in.execute(ctx)
    delete.expressions foreach { exp =>
      val projected = df.project(Seq(("delete", exp)))(ctx.expressionContext)
      val (_, elementType) = projected.schema.head
      elementType match {
        case CTNode =>
          graphModel.deleteNodesSafely(
            dropNull(projected.records) map { _.asInstanceOf[LynxNode].id },
            delete.forced
          )
        case CTRelationship =>
          graphModel.deleteRelations(dropNull(projected.records) map {
            _.asInstanceOf[LynxRelationship].id
          })
        case CTPath =>
        case _ =>
          throw SyntaxErrorException(s"expected Node, Path pr Relationship, but a ${elementType}")
      }
    }

    def dropNull(values: Iterator[Seq[LynxValue]]): Iterator[LynxValue] =
      values.flatMap(_.headOption.filterNot(LynxNull.equals))

    DataFrame.empty
  }

}

//////// SET ///////
case class PhysicalSetClauseTranslator(setItems: Seq[SetItem]) extends PhysicalNodeTranslator {
  override def translate(
      in: Option[PhysicalNode]
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    PhysicalSetClause(setItems)(in.get, ppc)
  }
}

case class PhysicalSetClause(
    var setItems: Seq[SetItem],
    mergeAction: Seq[MergeAction] = Seq.empty
  )(implicit val in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {

  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalSetClause =
    PhysicalSetClause(setItems)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)

    setItems = {
      if (mergeAction.nonEmpty) {
        val isCreate = plannerContext.pptContext("MergeAction").asInstanceOf[Boolean]
        if (isCreate)
          mergeAction.find(p => p.isInstanceOf[OnCreate]).get.asInstanceOf[OnCreate].action.items
        else mergeAction.find(p => p.isInstanceOf[OnMatch]).get.asInstanceOf[OnMatch].action.items
      } else setItems
    }

    // TODO: batch process , not Iterator(one) !!!
    val res = df.records.map(n => {
      val ctxMap = df.schema.zip(n).map(x => x._1._1 -> x._2).toMap

      n.size match {
        // set node
        case 1 => {
          var tmpNode = n.head.asInstanceOf[LynxNode]
          setItems.foreach {
            case sp @ SetPropertyItem(property, literalExpr) => {
              val Property(map, keyName) = property
              map match {
                case v @ Variable(name) => {
                  val data = Array(
                    keyName.name -> eval(literalExpr)(ctx.expressionContext.withVars(ctxMap)).value
                  )
                  tmpNode =
                    graphModel.setNodesProperties(Iterator(tmpNode.id), data, false).next().get
                }
                case cp @ CaseExpression(expression, alternatives, default) => {
                  val res = eval(cp)(ctx.expressionContext.withVars(ctxMap))
                  res match {
                    case LynxNull => tmpNode = n.head.asInstanceOf[LynxNode]
                    case _ => {
                      val data = Array(
                        keyName.name -> eval(literalExpr)(
                          ctx.expressionContext.withVars(ctxMap)
                        ).value
                      )
                      tmpNode = graphModel
                        .setNodesProperties(Iterator(res.asInstanceOf[LynxNode].id), data, false)
                        .next()
                        .get
                    }
                  }
                }
              }
            }
            case sl @ SetLabelItem(variable, labels) => {
              tmpNode = graphModel
                .setNodesLabels(Iterator(tmpNode.id), labels.map(f => f.name).toArray)
                .next()
                .get
            }
            case si @ SetIncludingPropertiesFromMapItem(variable, expression) => {
              expression match {
                case MapExpression(items) => {
                  val data = items.map(f =>
                    f._1.name -> eval(f._2)(ctx.expressionContext.withVars(ctxMap)).value
                  )
                  tmpNode = graphModel
                    .setNodesProperties(Iterator(tmpNode.id), data.toArray, false)
                    .next()
                    .get
                }
              }
            }
            case sep @ SetExactPropertiesFromMapItem(variable, expression) => {
              expression match {
                case MapExpression(items) => {
                  val data = items.map(f =>
                    f._1.name -> eval(f._2)(ctx.expressionContext.withVars(ctxMap)).value
                  )
                  tmpNode = graphModel
                    .setNodesProperties(Iterator(tmpNode.id), data.toArray, true)
                    .next()
                    .get
                }
              }
            }
          }
          Seq(tmpNode)
        }
        // set join TODO copyNode
        case 2 => {
          var tmpNode = Seq[LynxValue]()
          setItems.foreach {
            case sep @ SetExactPropertiesFromMapItem(variable, expression) => {
              expression match {
                case v @ Variable(name) => {
                  val srcNode = ctxMap(variable.name).asInstanceOf[LynxNode]
                  val maskNode = ctxMap(name).asInstanceOf[LynxNode]
                  //                  tmpNode = graphModel.copyNode(srcNode, maskNode, ctx.tx)
                }
              }
            }
          }
          tmpNode
        }
        // set relationship
        case 3 => {
          var triple = n
          setItems.foreach {
            case sp @ SetPropertyItem(property, literalExpr) => {
              val Property(variable, keyName) = property
              val data = Array(
                keyName.name -> eval(literalExpr)(ctx.expressionContext.withVars(ctxMap)).value
              )
              val newRel = graphModel
                .setRelationshipsProperties(
                  Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                  data
                )
                .next()
                .get
              triple = Seq(triple.head, newRel, triple.last)
            }
            case sl @ SetLabelItem(variable, labels) => {
              // TODO: An relation is able to have multi-type ???
              val newRel = graphModel
                .setRelationshipsType(
                  Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                  labels.map(f => f.name).toArray.head
                )
                .next()
                .get
              triple = Seq(triple.head, newRel, triple.last)
            }
            case si @ SetIncludingPropertiesFromMapItem(variable, expression) => {
              expression match {
                case MapExpression(items) => {
                  items.foreach(f => {
                    val data =
                      Array(f._1.name -> eval(f._2)(ctx.expressionContext.withVars(ctxMap)).value)
                    val newRel = graphModel
                      .setRelationshipsProperties(
                        Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                        data
                      )
                      .next()
                      .get
                    triple = Seq(triple.head, newRel, triple.last)
                  })
                }
              }
            }
          }
          triple
        }
      }
    })

    DataFrame.cached(schema, res.toSeq)
  }
}

////////////////////

/////////REMOVE//////////////
case class PhysicalRemoveTranslator(removeItems: Seq[RemoveItem]) extends PhysicalNodeTranslator {
  override def translate(
      in: Option[PhysicalNode]
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    PhysicalRemove(removeItems)(in.get, ppc)
  }
}

case class PhysicalRemove(
    removeItems: Seq[RemoveItem]
  )(implicit val in: PhysicalNode,
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {

  override val children: Seq[PhysicalNode] = Seq(in)

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalRemove =
    PhysicalRemove(removeItems)(children0.head, plannerContext)

  override val schema: Seq[(String, LynxType)] = in.schema

  override def execute(implicit ctx: ExecutionContext): DataFrame = {
    val df = in.execute(ctx)
    val res = df.records.map(n => {
      n.size match {
        case 1 => {
          var tmpNode: LynxNode = n.head.asInstanceOf[LynxNode]
          removeItems.foreach {
            case rp @ RemovePropertyItem(property) =>
              tmpNode = graphModel
                .removeNodesProperties(Iterator(tmpNode.id), Array(rp.property.propertyKey.name))
                .next()
                .get

            case rl @ RemoveLabelItem(variable, labels) =>
              tmpNode = graphModel
                .removeNodesLabels(Iterator(tmpNode.id), rl.labels.map(f => f.name).toArray)
                .next()
                .get
          }
          Seq(tmpNode)
        }
        case 3 => {
          var triple: Seq[LynxValue] = n
          removeItems.foreach {
            case rp @ RemovePropertyItem(property) => {
              val newRel = graphModel
                .removeRelationshipsProperties(
                  Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                  Array(property.propertyKey.name)
                )
                .next()
                .get
              triple = Seq(triple.head, newRel, triple.last)
            }

            case rl @ RemoveLabelItem(variable, labels) => {
              // TODO: An relation is able to have multi-type ???
              val newRel = graphModel
                .removeRelationshipType(
                  Iterator(triple(1).asInstanceOf[LynxRelationship].id),
                  labels.map(f => f.name).toArray.head
                )
                .next()
                .get
              triple = Seq(triple.head, newRel, triple.last)
            }
          }
          triple
        }
      }
    })
    DataFrame.cached(schema, res.toSeq)
  }
}

////////////////////////////

/////////UNWIND//////////////
case class PhysicalUnwindTranslator(expression: Expression, variable: Variable)
  extends PhysicalNodeTranslator {
  override def translate(
      in: Option[PhysicalNode]
    )(implicit ppc: PhysicalPlannerContext
    ): PhysicalNode = {
    PhysicalUnwind(expression, variable)(in, ppc)
  }
}

case class PhysicalUnwind(
    expression: Expression,
    variable: Variable
  )(implicit val in: Option[PhysicalNode],
    val plannerContext: PhysicalPlannerContext)
  extends AbstractPhysicalNode {
  override val children: Seq[PhysicalNode] = in.toSeq

  override val schema: Seq[(String, LynxType)] =
    in.map(_.schema).getOrElse(Seq.empty) ++ Seq((variable.name, CTAny)) // TODO it is CTAny?

  override def execute(implicit ctx: ExecutionContext): DataFrame = // fixme
    in map { inNode =>
      val df = inNode.execute(ctx) // dataframe of in
      val colName = schema map { case (name, _) => name }
      DataFrame(
        schema,
        () =>
          df.records flatMap { record =>
            val recordCtx = ctx.expressionContext.withVars(colName zip (record) toMap)
            val rsl = (expressionEvaluator.eval(expression)(recordCtx) match {
              case list: LynxList     => list.value
              case element: LynxValue => List(element)
            }) map { element =>
              record :+ element
            }
            rsl
          }
      )
    //      df.project(, Seq((variable.name, expression)))(ctx.expressionContext).records
    //        .flatten.flatMap{
    //        case list: LynxList => list.value
    //        case element: LynxValue => List(element)
    //      }.map(lv => Seq(lv))
    } getOrElse {
      DataFrame(
        schema,
        () =>
          eval(expression)(ctx.expressionContext).asInstanceOf[LynxList].value.toIterator map (
              lv => Seq(lv)
          )
      )
    }

  override def withChildren(children0: Seq[PhysicalNode]): PhysicalUnwind =
    PhysicalUnwind(expression, variable)(children0.headOption, plannerContext)
}
////////////////////////////

case class NodeInput(labels: Seq[LynxNodeLabel], props: Seq[(LynxPropertyKey, LynxValue)]) {}

case class RelationshipInput(
    types: Seq[LynxRelationshipType],
    props: Seq[(LynxPropertyKey, LynxValue)],
    startNodeRef: NodeInputRef,
    endNodeRef: NodeInputRef) {}

sealed trait NodeInputRef

case class StoredNodeInputRef(id: LynxId) extends NodeInputRef

case class ContextualNodeInputRef(varname: String) extends NodeInputRef

case class UnresolvableVarException(var0: Option[LogicalVariable]) extends LynxException

case class SyntaxErrorException(msg: String) extends LynxException
