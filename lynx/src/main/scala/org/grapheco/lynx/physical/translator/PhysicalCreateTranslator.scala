package org.grapheco.lynx.physical.translator

import org.grapheco.lynx.physical.{CreateElement, CreateNode, CreateRelationship, PhysicalCreate, PhysicalNode}
import org.grapheco.lynx.planner.PhysicalPlannerContext
import org.grapheco.lynx.{LynxType}
import org.grapheco.tudb.exception.{TuDBError, TuDBException}
import org.opencypher.v9_0.ast.Create
import org.opencypher.v9_0.expressions.{EveryPath, Expression, LabelName, LogicalVariable, NodePattern, Range, RelTypeName, RelationshipChain, RelationshipPattern, SemanticDirection}
import org.opencypher.v9_0.util.symbols.{CTNode, CTRelationship}

import scala.collection.mutable.ArrayBuffer

/**
  *@description:
  */
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
          schema ++ Seq(varRelation -> CTRelationship) ++
            (if (!definedVars.contains(varRightNode)) {
               Seq(varRightNode -> CTNode)
             } else {
               Seq.empty
             }),
          ops ++
            (if (!definedVars.contains(varRightNode)) {
               Seq(CreateNode(varRightNode, labels3, properties3))
             } else {
               Seq.empty
             }) ++
            Seq(
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
