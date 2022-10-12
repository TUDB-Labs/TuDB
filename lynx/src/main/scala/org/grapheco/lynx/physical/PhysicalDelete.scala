// Copyright 2022 The TuDB Authors. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.grapheco.lynx.physical

import org.grapheco.lynx.physical.plan.PhysicalPlannerContext
import org.grapheco.lynx.{DataFrame, ExecutionContext, LynxType, SyntaxErrorException}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxNull
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.opencypher.v9_0.ast.Delete
import org.opencypher.v9_0.util.symbols.{CTNode, CTPath, CTRelationship}

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
