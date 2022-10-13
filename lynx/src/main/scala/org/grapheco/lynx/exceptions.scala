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

package org.grapheco.lynx

import org.opencypher.v9_0.expressions.LogicalVariable
import org.opencypher.v9_0.util.ASTNode

/**
  *@description:
  */
trait LynxException extends RuntimeException {}

case class UnknownASTNodeException(node: ASTNode) extends LynxException

case class ParsingException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class ConstrainViolatedException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class NoIndexManagerException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class UnresolvableVarException(var0: Option[LogicalVariable]) extends LynxException

case class SyntaxErrorException(msg: String) extends LynxException

case class LynxProcedureException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class UnknownProcedureException(prefix: List[String], name: String) extends LynxException {
  override def getMessage: String = s"unknown procedure: ${(prefix :+ name).mkString(".")}"
}

case class ProcedureUnregisteredException(msg: String) extends LynxException {
  override def getMessage: String = msg
}

case class WrongArgumentException(
    argName: String,
    expectedTypes: Seq[LynxType],
    actualTypes: Seq[LynxType])
  extends LynxException {
  override def getMessage: String =
    s"Wrong argument of $argName, expected: ${expectedTypes.mkString(", ")}, actual: ${actualTypes.mkString(", ")}."
}

case class WrongNumberOfArgumentsException(signature: String, sizeExpected: Int, sizeActual: Int)
  extends LynxException {
  override def getMessage: String =
    s"Wrong number of arguments of $signature(), expected: $sizeExpected, actual: ${sizeActual}."
}
