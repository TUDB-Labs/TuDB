package org.grapheco.tudb.cmd

import com.typesafe.scalalogging.LazyLogging
import org.slf4j.LoggerFactory

class GraphAPICmd() extends LazyLogging {

  val LOGGER = LoggerFactory.getLogger("graph-api-cmd-info")
  // Class variables
  var number: Int = 16
  var nameofcompany: String = "Apple"

  // Class method
  def Display()
  {
    println("Name of the company : " + nameofcompany);
    println("Total number of Smartphone generation: " + number);
  }
}

object Main {
  def main(args: Array[String])
  {

    // Class object
    var obj = new GraphAPICmd();
    obj.Display();
  }
}