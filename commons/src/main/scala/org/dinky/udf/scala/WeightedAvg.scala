package org.dinky.udf.scala

import org.apache.flink.table.catalog.DataTypeFactory
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.types.extraction.TypeInferenceExtractor
import org.apache.flink.table.types.inference.TypeInference

import java.lang.{Integer => JInteger, Long => JLong}

/**
 * Accumulator for WeightedAvg.
 */
class WeightedAvgAccum {
  var sum: JLong = 0L
  var count: JLong = 0
}

/**
 * Weighted Average user-defined aggregate function.
 */
class WeightedAvg extends AggregateFunction[JLong, WeightedAvgAccum] {

  override def createAccumulator(): WeightedAvgAccum = {
    new WeightedAvgAccum
  }

  override def getValue(acc: WeightedAvgAccum): JLong = {
    if (acc.count == 0) {
      null
    } else {
      acc.sum / acc.count
    }
  }

  def accumulate(acc: WeightedAvgAccum, iValue: JLong, iWeight: JInteger): Unit = {
    acc.sum += iValue * iWeight
    acc.count += iWeight
  }

  def retract(acc: WeightedAvgAccum, iValue: JLong, iWeight: JInteger): Unit = {
    acc.sum -= iValue * iWeight
    acc.count -= iWeight
  }

  def merge(acc: WeightedAvgAccum, it: java.lang.Iterable[WeightedAvgAccum]): Unit = {
    val iter = it.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.count += a.count
      acc.sum += a.sum
    }
  }

  def resetAccumulator(acc: WeightedAvgAccum): Unit = {
    acc.count = 0
    acc.sum = 0L
  }

  override def getTypeInference(typeFactory: DataTypeFactory): TypeInference = {
    return TypeInferenceExtractor.forAggregateFunction(typeFactory, classOf[WeightedAvg])

  }
}