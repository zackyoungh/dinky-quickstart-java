package org.dinky.udf.scala

import org.apache.flink.table.functions.ScalarFunction

import scala.annotation.varargs

// 有多个重载求值方法的函数
class SumFunction extends ScalarFunction {

  def eval(a: Integer, b: Integer): Integer = {
    a + b
  }

  def eval(a: String, b: String): Integer = {
    Integer.valueOf(a) + Integer.valueOf(b)
  }

  @varargs // generate var-args like Java
  def eval(d: Double*): Integer = {
    d.sum.toInt
  }
}
