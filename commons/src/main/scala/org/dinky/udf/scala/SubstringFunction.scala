package org.dinky.udf.scala

import org.apache.flink.table.functions.ScalarFunction

class SubstringFunction extends ScalarFunction {
  def eval(s: String, begin: Integer, end: Integer): String = {
    println("this is scala")
    s.substring(begin, end)
  }
}

