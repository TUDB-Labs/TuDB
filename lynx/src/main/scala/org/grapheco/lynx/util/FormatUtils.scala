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

package org.grapheco.lynx.util

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.StringUtils

object FormatUtils {
  def format(x: Any, nullString: String = "(null)"): String = {
    x match {
      case null       => nullString
      case None       => nullString
      case Some(m)    => format(m, nullString);
      case date: Date => new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").format(date)
      case _          => x.toString
    }
  }

  def printTable(columns: Seq[String], data: Seq[Seq[Any]], nullString: String = "(null)") = {
    val formatedColumns: Seq[String] = columns.map(format(_, nullString))
    val formatedData: Seq[Seq[String]] = data.map(_.map(format(_, nullString)))

    val sb = new StringBuilder
    val numCols = formatedColumns.length

    //width>=3
    val colWidths = Array.fill(numCols)(3)

    //width of each column
    for (row ← formatedData :+ formatedColumns) {
      for ((cell, i) ← row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }

    // Create SeparateLine
    colWidths.map("═" * _).addString(sb, "╔", "╤", "╗\n")

    // column names
    formatedColumns.zipWithIndex
      .map {
        case (cell, i) ⇒
          StringUtils.rightPad(cell, colWidths(i))
      }
      .addString(sb, "║", "│", "║\n")

    colWidths.map("═" * _).addString(sb, "╠", "╪", "╣\n")

    // data
    formatedData.foreach {
      _.zipWithIndex
        .map {
          case (cell, i) ⇒
            StringUtils.rightPad(cell, colWidths(i))
        }
        .addString(sb, "║", "│", "║\n")
    }

    colWidths.map("═" * _).addString(sb, "╚", "╧", "╝\n")

    println(sb)
  }
}
