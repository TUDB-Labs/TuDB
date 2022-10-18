package org.grapheco.lynx

import org.grapheco.lynx.expression.{LynxAdd, LynxAnd, LynxAnds, LynxCaseExpression, LynxContainerIndex, LynxContains, LynxEndsWith, LynxEquals, LynxGreaterThan, LynxGreaterThanOrEqual, LynxHasLabels, LynxIn, LynxIntegerLiteral, LynxIsNotNull, LynxIsNull, LynxLessThan, LynxLessThanOrEqual, LynxListLiteral, LynxLiteral, LynxMapExpression, LynxMultiRelationshipPathStep, LynxMultiply, LynxNilPathStep, LynxNodePathStep, LynxNot, LynxNotEquals, LynxOr, LynxOrs, LynxParameter, LynxPathExpression, LynxPathStep, LynxProperty, LynxRegexMatch, LynxSingleRelationshipPathStep, LynxStartsWith, LynxSubtract, LynxVariable}
import org.grapheco.lynx.procedure.{ProcedureExpression, ProcedureRegistry}
import org.grapheco.lynx.types.composite.{LynxList, LynxMap}
import org.grapheco.lynx.types.{LynxValue, TypeSystem}
import org.grapheco.lynx.types.property.{LynxBoolean, LynxFloat, LynxInteger, LynxNull, LynxNumber, LynxString}
import org.grapheco.lynx.types.structural.{HasProperty, LynxNode, LynxPropertyKey}
import org.grapheco.lynx.types.time.LynxDateTime
import org.opencypher.v9_0.expressions.Expression

import scala.util.matching.Regex

/**
  *@description: Temporary implementation, will be merged with eval function in the future.
  */
class LynxEval(
    typeSystem: TypeSystem,
    procedures: ProcedureRegistry,
    defaultEvaluate: DefaultExpressionEvaluator) {

  private def safeBinaryOp(
      lhs: Expression,
      rhs: Expression,
      op: (LynxValue, LynxValue) => LynxValue
    )(implicit ec: ExpressionContext
    ): Option[LynxValue] = {
    val l = eval(lhs)
    if (l.value == null) return None
    val r = eval(rhs)
    if (r.value == null) return None
    Some(op(l, r))
  }

  private def evalPathStep(step: LynxPathStep)(implicit ec: ExpressionContext): LynxValue = {
    step match {
      case LynxNilPathStep     => LynxList(List.empty)
      case f: LynxNodePathStep => LynxList(List(eval(f.node), evalPathStep(f.next)))
      case m: LynxMultiRelationshipPathStep =>
        LynxList(List(eval(m.rel), eval(m.toNode.get), evalPathStep(m.next)))
      case s: LynxSingleRelationshipPathStep =>
        LynxList(
          s.dependencies.map(eval).toList ++ List(eval(s.toNode.get)) ++ List(evalPathStep(s.next))
        )
    }
  }

  def eval(expr: Expression)(implicit ec: ExpressionContext): LynxValue = {
    expr match {
      case LynxHasLabels(expression, labels) =>
        eval(expression) match {
          case node: LynxNode =>
            LynxBoolean(labels.forall(label => node.labels.map(_.value).contains(label.value)))
        }

      case fe: ProcedureExpression => {
        if (fe.aggregating) LynxValue(fe.args.map(eval(_)))
        else fe.procedure.call(fe.args.map(eval(_)))
      }

      case ps: LynxPathExpression => evalPathStep(ps.steps)

      // pattern like: match (n) WHERE n[toLower(propname)] < 30 return n
      case LynxContainerIndex(expr, idx) => {
        {
          (eval(expr), eval(idx)) match {
            case (hp: HasProperty, i: LynxString) => hp.property(LynxPropertyKey(i.value))
            case (lm: LynxMap, key: LynxString)   => lm.value.get(key.value)
            case (lm: LynxList, i: LynxInteger)   => lm.value.lift(i.value.toInt)
          }
        }.getOrElse(LynxNull)
      }

      case LynxAdd(lhs, rhs) =>
        safeBinaryOp(
          lhs,
          rhs,
          (lvalue, rvalue) =>
            (lvalue, rvalue) match {
              case (a: LynxNumber, b: LynxNumber) => a + b
              case (a: LynxString, b: LynxString) => LynxString(a.value + b.value)
              case (a: LynxList, b: LynxList)     => LynxList(a.value ++ b.value)
            }
        ).getOrElse(LynxNull)

      case LynxSubtract(lhs, rhs) =>
        safeBinaryOp(
          lhs,
          rhs,
          (lvalue, rvalue) =>
            (lvalue, rvalue) match {
              case (a: LynxNumber, b: LynxNumber) => a - b
            }
        ).getOrElse(LynxNull)

      case LynxOrs(exprs) =>
        LynxBoolean(exprs.exists(eval(_).value == true))

      case LynxAnds(exprs) =>
        LynxBoolean(exprs.forall(eval(_).value == true))

      case LynxOr(lhs, rhs) =>
        LynxBoolean(eval(lhs).value == true || eval(rhs).value == true)

      case LynxAnd(lhs, rhs) =>
        LynxBoolean(eval(lhs).value == true && eval(rhs).value == true)

      case LynxMultiply(lhs, rhs) => { //todo add normal multi
        (eval(lhs), eval(rhs)) match {
          case (n: LynxNumber, m: LynxInteger) => { //todo add aggregating multi
            n match {
              case d: LynxFloat   => LynxFloat(d.value * m.value)
              case d: LynxInteger => LynxInteger(d.value * m.value)
            }
          }
        }
      }

      case LynxNotEquals(lhs, rhs) => //todo add testcase: 1) n.name == null 2) n.nullname == 'some'
        LynxValue(eval(lhs) != eval(rhs))

      case LynxEquals(lhs, rhs) =>
        (eval(lhs), eval(rhs)) match {
          case (a: LynxNumber, b: LynxNumber) => LynxValue(a.value == b.value)
          case (a: LynxValue, b: LynxValue)   => LynxValue(a == b)
        }

      case LynxGreaterThan(lhs, rhs) =>
        safeBinaryOp(
          lhs,
          rhs,
          (lvalue, rvalue) => {
            (lvalue, rvalue) match {
              case (a: LynxNumber, b: LynxNumber) =>
                LynxBoolean(a.number.doubleValue() > b.number.doubleValue())
              case (a: LynxString, b: LynxString) => LynxBoolean(a.value > b.value)
            }
          }
        ).getOrElse(LynxNull)

      case LynxGreaterThanOrEqual(lhs, rhs) =>
        safeBinaryOp(
          lhs,
          rhs,
          (lvalue, rvalue) => {
            (lvalue, rvalue) match {
              case (a: LynxNumber, b: LynxNumber) =>
                LynxBoolean(a.number.doubleValue() >= b.number.doubleValue())
              case (a: LynxString, b: LynxString) => LynxBoolean(a.value >= b.value)
            }
          }
        ).getOrElse(LynxNull)

      case LynxLessThan(lhs, rhs) =>
        eval(LynxGreaterThan(rhs, lhs))

      case LynxLessThanOrEqual(lhs, rhs) =>
        eval(LynxGreaterThanOrEqual(rhs, lhs))

      case LynxNot(in) =>
        eval(in) match {
          case LynxNull       => LynxBoolean(false)
          case LynxBoolean(b) => LynxBoolean(!b)
        }

      case LynxIsNull(lhs) => {
        eval(lhs) match {
          case LynxNull => LynxBoolean(true)
          case _        => LynxBoolean(false)
        }
      }
      case LynxIsNotNull(lhs) => {
        eval(lhs) match {
          case LynxNull => LynxBoolean(false)
          case _        => LynxBoolean(true)
        }
      }

      case v: LynxLiteral =>
        typeSystem.wrap(v.value)

      case v: LynxListLiteral =>
        LynxValue(v.expressions.map(eval(_)))

      case LynxVariable(name, columnOffset) =>
        ec.vars(name)

      case LynxProperty(src, LynxPropertyKey(name)) =>
        eval(src) match {
          case LynxNull           => LynxNull
          case hp: HasProperty    => hp.property(LynxPropertyKey(name)).getOrElse(LynxNull)
          case time: LynxDateTime => defaultEvaluate.timeEvalSupport(time, name)
        }

      case LynxIn(lhs, rhs) =>
        eval(rhs) match {
          case LynxList(list) =>
            LynxBoolean(list.contains(eval(lhs))) //todo add literal in list[func] test case
        }

      case LynxParameter(name, parameterType) =>
        typeSystem.wrap(ec.param(name))

      case LynxRegexMatch(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (LynxString(str), LynxString(regStr)) => {
            val regex = new Regex(regStr) // TODO: opt
            val res = regex.findFirstMatchIn(str)
            if (res.isDefined) LynxBoolean(true)
            else LynxBoolean(false)
          }
          case (LynxNull, _) => LynxBoolean(false)
        }
      }

      case LynxStartsWith(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (LynxString(str), LynxString(startStr)) => LynxBoolean(str.startsWith(startStr))
          case (LynxNull, _)                           => LynxBoolean(false)
        }
      }

      case LynxEndsWith(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (LynxString(str), LynxString(endStr)) => LynxBoolean(str.endsWith(endStr))
          case (LynxNull, _)                         => LynxBoolean(false)
        }
      }

      case LynxContains(lhs, rhs) => {
        (eval(lhs), eval(rhs)) match {
          case (LynxString(str), LynxString(containsStr)) => LynxBoolean(str.contains(containsStr))
          case (LynxNull, _)                              => LynxBoolean(false)
        }
      }

      case LynxCaseExpression(expression, alternatives, default) => {
        if (expression.isDefined) {
          val evalValue = eval(expression.get)
          evalValue match {
            case LynxNull => LynxNull
            case _ => {
              val expr = alternatives
                .find(alt => {
                  // case [xxx] when [yyy] then 1
                  // if [yyy] is a boolean, then [xxx] no use
                  val res = eval(alt._1)
                  if (res.isInstanceOf[LynxBoolean]) res.value.asInstanceOf[Boolean]
                  else eval(alt._1) == evalValue
                })
                .map(_._2)
                .getOrElse(default.get)

              eval(expr)
            }
          }
        } else {
          val expr =
            alternatives.find(alt => eval(alt._1).value.asInstanceOf[Boolean]).map(_._2).getOrElse {
              default.orNull
            }
          if (expr != null) eval(expr)
          else LynxNull
        }
      }

      case LynxMapExpression(items) => LynxMap(items.map(it => it._1.value -> eval(it._2)).toMap)
    }
  }
}
