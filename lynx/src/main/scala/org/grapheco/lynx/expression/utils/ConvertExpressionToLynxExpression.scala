package org.grapheco.lynx.expression.utils

import org.grapheco.lynx.expression.{LynxCaseExpression, LynxCountStar, LynxExpression, LynxHasLabels, LynxListLiteral, LynxMapExpression, LynxNodePathStep, LynxOperatorExpression, LynxParameter, LynxPathExpression, LynxProperty, LynxVariable}
import org.grapheco.lynx.procedure.ProcedureExpression
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey}
import org.opencypher.v9_0.expressions.{CaseExpression, CountStar, Expression, FunctionInvocation, HasLabels, ListLiteral, Literal, MapExpression, NodePathStep, OperatorExpression, Parameter, PathExpression, Property, Variable}

/**
  *@description:
  */
object ConvertExpressionToLynxExpression {
  def convert(expr: Expression): LynxExpression = {
    expr match {
      // TODO: calculate columnOffset
      case Variable(name) => LynxVariable(name, 0)

      case HasLabels(expression, labels) =>
        LynxHasLabels(
          ConvertExpressionToLynxExpression.convert(expression),
          labels.map(l => LynxNodeLabel(l.name))
        )

      case CountStar() => LynxCountStar()

      case CaseExpression(expression, alternatives, default) =>
        LynxCaseExpression(
          expression.map(expr => ConvertExpressionToLynxExpression.convert(expr)),
          alternatives.map(exprs =>
            (
              ConvertExpressionToLynxExpression.convert(exprs._1),
              ConvertExpressionToLynxExpression.convert(exprs._2)
            )
          ),
          default.map(expr => ConvertExpressionToLynxExpression.convert(expr))
        )

      case Property(map, propertyKey) =>
        LynxProperty(convert(map), LynxPropertyKey(propertyKey.name))

      case PathExpression(step) => LynxPathExpression(ConvertPathStepToLynxPathStep.convert(step))

      case literal: Literal => ConvertLiteralToLynxLiteral.convert(literal)

      case lstLiteral: ListLiteral =>
        LynxListLiteral(
          lstLiteral.expressions.map(expr => ConvertExpressionToLynxExpression.convert(expr))
        )

      case opExpression: OperatorExpression =>
        ConvertPredicateExpressionToLynxExpression.convert(opExpression)

      case Parameter(name, parameterType) => LynxParameter(name, parameterType)

      case MapExpression(items) =>
        LynxMapExpression(
          items.map(kv =>
            (LynxPropertyKey(kv._1.name), ConvertExpressionToLynxExpression.convert(kv._2))
          )
        )

      case expression: LynxExpression => expression
    }
  }
}
