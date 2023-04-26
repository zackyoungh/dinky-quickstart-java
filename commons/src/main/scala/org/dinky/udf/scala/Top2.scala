package org.dinky.udf.scala

import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.table.functions.TableAggregateFunction.RetractableCollector

import java.lang.{Integer => JInteger}

/**
 * Accumulator for top2.
 */
class Top2Accum {
  var first: JInteger = _
  var second: JInteger = _
  var oldFirst: JInteger = _
  var oldSecond: JInteger = _
}

/**
 * The top2 user-defined table aggregate function.
 */
class Top2 extends TableAggregateFunction[(JInteger, JInteger), Top2Accum] {

  override def createAccumulator(): Top2Accum = {
    val acc = new Top2Accum
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc.oldFirst = Int.MinValue
    acc.oldSecond = Int.MinValue
    acc
  }

  def accumulate(acc: Top2Accum, v: Int) {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def emitUpdateWithRetract(
                             acc: Top2Accum,
                             out: RetractableCollector[(JInteger, JInteger)])
  : Unit = {
    if (acc.first != acc.oldFirst) {
      // if there is an update, retract old value then emit new value.
      if (acc.oldFirst != Int.MinValue) {
        out.retract((acc.oldFirst, 1))
      }
      out.collect((acc.first, 1))
      acc.oldFirst = acc.first
    }
    if (acc.second != acc.oldSecond) {
      // if there is an update, retract old value then emit new value.
      if (acc.oldSecond != Int.MinValue) {
        out.retract((acc.oldSecond, 2))
      }
      out.collect((acc.second, 2))
      acc.oldSecond = acc.second
    }
  }
}

