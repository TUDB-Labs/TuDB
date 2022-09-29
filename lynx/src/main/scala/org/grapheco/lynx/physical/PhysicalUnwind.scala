package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.composite.LynxList
import org.opencypher.v9_0.expressions.{Expression, Variable}
import org.opencypher.v9_0.util.symbols.CTAny

/**
  *@description:
  */
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
