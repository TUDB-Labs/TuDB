package org.grapheco.lynx

import org.opencypher.v9_0.ast.{AliasedReturnItem, Clause, Create, CreateIndex, CreateUniquePropertyConstraint, Delete, Limit, Match, Merge, MergeAction, OrderBy, PeriodicCommitHint, ProcedureResult, ProcedureResultItem, Query, QueryPart, Remove, Return, ReturnItem, ReturnItems, ReturnItemsDef, SetClause, SingleQuery, Skip, SortItem, Statement, UnresolvedCall, Unwind, Where, With}
import org.opencypher.v9_0.expressions.{EveryPath, Expression, FunctionInvocation, FunctionName, LabelName, LogicalVariable, Namespace, NodePattern, Pattern, PatternElement, PatternPart, ProcedureName, Property, PropertyKeyName, RelationshipChain, RelationshipPattern, Variable}
import org.opencypher.v9_0.util.{ASTNode, InputPosition}

//logical plan tree node (operator)
trait LogicalNode extends TreeNode {
  override type SerialType = LogicalNode
  override val children: Seq[LogicalNode] = Seq.empty
}

trait LogicalPlanner {
  def plan(statement: Statement, plannerContext: LogicalPlannerContext): LogicalNode
}

//translates an ASTNode into a LogicalNode, `in` as input operator
trait LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode
}

//pipelines a set of LogicalNodes
case class PipedTranslators(items: Seq[LogicalNodeTranslator]) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    items
      .foldLeft[Option[LogicalNode]](in) { (in, item) =>
        Some(item.translate(in)(plannerContext))
      }
      .get
  }
}

class DefaultLogicalPlanner(runnerContext: CypherRunnerContext) extends LogicalPlanner {
  private def translate(node: ASTNode)(implicit lpc: LogicalPlannerContext): LogicalNode = {
    node match {
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        LogicalQueryPartTranslator(part).translate(None)

      case CreateUniquePropertyConstraint(
          Variable(v1),
          LabelName(l),
          List(Property(Variable(v2), PropertyKeyName(p)))
          ) =>
        throw UnknownASTNodeException(node)

      case CreateIndex(labelName, properties) =>
        LogicalCreateIndex(labelName, properties)

      case _ =>
        throw UnknownASTNodeException(node)
    }
  }

  override def plan(statement: Statement, plannerContext: LogicalPlannerContext): LogicalNode = {
    translate(statement)(plannerContext)
  }
}

/////////////////ProcedureCall/////////////
case class LogicalProcedureCallTranslator(c: UnresolvedCall) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    val UnresolvedCall(
      ns @ Namespace(parts: List[String]),
      pn @ ProcedureName(name: String),
      declaredArguments: Option[Seq[Expression]],
      declaredResult: Option[ProcedureResult]
    ) = c
    val call = LogicalProcedureCall(ns, pn, declaredArguments)

    declaredResult match {
      case Some(ProcedureResult(items: IndexedSeq[ProcedureResultItem], where: Option[Where])) =>
        PipedTranslators(
          Seq(
            LogicalSelectTranslator(items.map(item => {
              val ProcedureResultItem(output, Variable(varname)) = item
              varname -> output.map(_.name)
            })),
            LogicalWhereTranslator(where)
          )
        ).translate(Some(call))

      case None => call
    }
  }
}

case class LogicalProcedureCall(
    procedureNamespace: Namespace,
    procedureName: ProcedureName,
    declaredArguments: Option[Seq[Expression]])
  extends LogicalNode

//////////////////Create////////////////
case class LogicalCreateTranslator(c: Create) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
    LogicalCreate(c)(in)
}

case class LogicalCreate(c: Create)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}

//////////////merge//////////////////////
case class LogicalMergeTranslator(m: Merge) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    val matchInfo = Match(false, m.pattern, Seq.empty, m.where)(m.position)
    val mergeIn = LogicalMatchTranslator(matchInfo).translate(in)
    val mergeInfo = LogicalMerge(m)(Option(mergeIn))

    if (m.actions.nonEmpty) LogicalMergeAction(m.actions)(Option(mergeInfo))
    else mergeInfo
  }
}
case class LogicalMerge(m: Merge)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
case class LogicalMergeAction(m: Seq[MergeAction])(val in: Option[LogicalNode])
  extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
///////////////////////////////////////

//////////////////Delete////////////////
case class LogicalDeleteTranslator(delete: Delete) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
    LogicalDelete(delete)(in.get)
}

case class LogicalDelete(delete: Delete)(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}
///////////////////////////////////////

//////////////////Set////////////////
case class LogicalSetClauseTranslator(s: SetClause) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
    LogicalSetClause(s)(in)
}

case class LogicalSetClause(d: SetClause)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq

}
///////////////////////////////////////

//////////////REMOVE//////////////////
case class LogicalRemoveTranslator(r: Remove) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
    LogicalRemove(r)(in)
}

case class LogicalRemove(r: Remove)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
/////////////////////////////////////

//////////////UNWIND//////////////////
case class LogicalUnwindTranslator(u: Unwind) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode =
//    in match {
//      case None => LogicalUnwind(u)(None)
//      case Some(left) => LogicalJoin(isSingleMatch = false)(left, LogicalUnwind(u)(in))
//    }
    LogicalUnwind(u)(in)
}

case class LogicalUnwind(u: Unwind)(val in: Option[LogicalNode]) extends LogicalNode {
  override val children: Seq[LogicalNode] = in.toSeq
}
/////////////////////////////////////

case class LogicalQueryPartTranslator(part: QueryPart) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    part match {
      case SingleQuery(clauses: Seq[Clause]) =>
        PipedTranslators(
          clauses.map {
            case c: UnresolvedCall => LogicalProcedureCallTranslator(c)
            case r: Return         => LogicalReturnTranslator(r)
            case w: With           => LogicalWithTranslator(w)
            case u: Unwind         => LogicalUnwindTranslator(u)
            case m: Match          => LogicalMatchTranslator(m)
            case c: Create         => LogicalCreateTranslator(c)
            case m: Merge          => LogicalMergeTranslator(m)
            case d: Delete         => LogicalDeleteTranslator(d)
            case s: SetClause      => LogicalSetClauseTranslator(s)
            case r: Remove         => LogicalRemoveTranslator(r)
          }
        ).translate(in)
    }
  }
}

///////////////with,return////////////////
case class LogicalReturnTranslator(r: Return) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    val Return(distinct, ri, orderBy, skip, limit, excludedNames) = r

    PipedTranslators(
      Seq(
        LogicalProjectTranslator(ri),
        LogicalSkipTranslator(skip),
        LogicalLimitTranslator(limit),
        LogicalOrderByTranslator(orderBy),
        LogicalSelectTranslator(ri),
        LogicalDistinctTranslator(distinct)
      )
    ).translate(in)
  }
}

case class LogicalOrderByTranslator(orderBy: Option[OrderBy]) extends LogicalNodeTranslator {
  override def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    orderBy match {
      case None        => in.get
      case Some(value) => LogicalOrderBy(value.sortItems)(in.get)
    }
  }

}

case class LogicalOrderBy(sortItem: Seq[SortItem])(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}
object LogicalSelectTranslator {
  def apply(ri: ReturnItemsDef): LogicalSelectTranslator = LogicalSelectTranslator(
    ri.items.map(item => item.name -> item.alias.map(_.name))
  )
}

case class LogicalSelectTranslator(columns: Seq[(String, Option[String])])
  extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    LogicalSelect(columns)(in.get)
  }
}

case class LogicalSelect(columns: Seq[(String, Option[String])])(val in: LogicalNode)
  extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}

case class LogicalProject(ri: ReturnItemsDef)(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}

case class LogicalAggregation(
    aggregatings: Seq[ReturnItem],
    groupings: Seq[ReturnItem]
  )(val in: LogicalNode)
  extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}

case class LogicalLimit(expression: Expression)(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}

case class LogicalSkip(expression: Expression)(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}

case class LogicalFilter(expr: Expression)(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}

case class LogicalWhereTranslator(where: Option[Where]) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    where match {
      case None              => in.get
      case Some(Where(expr)) => LogicalFilter(expr)(in.get)
    }
  }
}

case class LogicalWithTranslator(w: With) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    (w, in) match {
      case (
          With(
            distinct,
            ReturnItems(includeExisting, items),
            orderBy,
            skip,
            limit: Option[Limit],
            where
          ),
          None
          ) =>
        LogicalCreateUnit(items)

      case (
          With(
            distinct,
            ri: ReturnItems,
            orderBy,
            skip: Option[Skip],
            limit: Option[Limit],
            where: Option[Where]
          ),
          Some(sin)
          ) =>
        PipedTranslators(
          Seq(
            LogicalProjectTranslator(ri),
            LogicalWhereTranslator(where),
            LogicalSkipTranslator(skip),
            LogicalOrderByTranslator(orderBy),
            LogicalLimitTranslator(limit),
            LogicalSelectTranslator(ri),
            LogicalDistinctTranslator(distinct)
          )
        ).translate(in)
    }
  }
}

case class LogicalCreateUnit(items: Seq[ReturnItem]) extends LogicalNode {}

case class LogicalProjectTranslator(ri: ReturnItemsDef) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    val newIn = in.getOrElse(LogicalCreateUnit(ri.items))
    if (ri.containsAggregate) {
      val (aggregatingItems, groupingItems) =
        ri.items.partition(i => i.expression.containsAggregate)
      LogicalAggregation(aggregatingItems, groupingItems)(newIn)
    } else {
      LogicalProject(ri)(newIn)
    }
  }
}

case class LogicalLimitTranslator(limit: Option[Limit]) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    limit match {
      case None              => in.get
      case Some(Limit(expr)) => LogicalLimit(expr)(in.get)
    }
  }
}

case class LogicalDistinctTranslator(distinct: Boolean) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    distinct match {
      case false => in.get
      case true  => LogicalDistinct()(in.get)
    }
  }
}

case class LogicalCreateIndex(labelName: LabelName, properties: List[PropertyKeyName])
  extends LogicalNode

trait LogicalQueryPart extends LogicalNode
case class LogicalSkipTranslator(skip: Option[Skip]) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    skip match {
      case None             => in.get
      case Some(Skip(expr)) => LogicalSkip(expr)(in.get)
    }
  }
}

case class LogicalDistinct()(val in: LogicalNode) extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(in)
}

///////////////////////////////////////
case class LogicalJoin(val isSingleMatch: Boolean)(val a: LogicalNode, val b: LogicalNode)
  extends LogicalNode {
  override val children: Seq[LogicalNode] = Seq(a, b)
}

/////////////////match/////////////////
case class LogicalPatternMatch(
    headNode: NodePattern,
    chain: Seq[(RelationshipPattern, NodePattern)])
  extends LogicalNode {}

case class LogicalMatchTranslator(m: Match) extends LogicalNodeTranslator {
  def translate(
      in: Option[LogicalNode]
    )(implicit plannerContext: LogicalPlannerContext
    ): LogicalNode = {
    //run match
    val Match(optional, Pattern(patternParts: Seq[PatternPart]), hints, where: Option[Where]) = m
    val parts = patternParts.map(matchPatternPart(_)(plannerContext))
    val matched = parts.drop(1).foldLeft(parts.head)((a, b) => LogicalJoin(true)(a, b))
    val filtered = LogicalWhereTranslator(where).translate(Some(matched))

    in match {
      case None       => filtered
      case Some(left) => LogicalJoin(false)(left, filtered)
    }
  }

  private def matchPatternPart(
      patternPart: PatternPart
    )(implicit lpc: LogicalPlannerContext
    ): LogicalNode = {
    patternPart match {
      case EveryPath(element: PatternElement) => matchPattern(element)
    }
  }

  private def matchPattern(
      element: PatternElement
    )(implicit lpc: LogicalPlannerContext
    ): LogicalPatternMatch = {
    element match {
      //match (m:label1)
      case np: NodePattern =>
        LogicalPatternMatch(np, Seq.empty)

      //match ()-[]->()
      case rc @ RelationshipChain(
            leftNode: NodePattern,
            relationship: RelationshipPattern,
            rightNode: NodePattern
          ) =>
        LogicalPatternMatch(leftNode, Seq(relationship -> rightNode))

      //match ()-[]->()-...-[r:type]->(n:label2)
      case rc @ RelationshipChain(
            leftChain: RelationshipChain,
            relationship: RelationshipPattern,
            rightNode: NodePattern
          ) =>
        val mp = matchPattern(leftChain)
        LogicalPatternMatch(mp.headNode, mp.chain :+ (relationship -> rightNode))
    }
  }
}

case class UnknownASTNodeException(node: ASTNode) extends LynxException
