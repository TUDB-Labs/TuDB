package org.grapheco.lynx.expression

import org.grapheco.lynx.types.structural.LynxPropertyKey

/**
  *@description:
  */
case class LynxMapExpression(items: Seq[(LynxPropertyKey, LynxExpression)]) extends LynxExpression {}
