package org.dinky.udf.scala

import cn.hutool.core.convert.Convert
import cn.hutool.core.io.BufferUtil
import org.apache.flink.table.annotation.{DataTypeHint, InputGroup}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

class OverloadedFunction extends ScalarFunction {

  // no hint required
  def eval(a: Long, b: Long): Long = {
    a + b
  }

  // 定义 decimal 的精度和小数位
  @DataTypeHint("DECIMAL(12, 3)")
  def eval(a: Double, b: Double): BigDecimal = {
    BigDecimal.valueOf(a + b)
  }

  // 定义嵌套数据类型
  @DataTypeHint("ROW<s STRING, t TIMESTAMP_LTZ(3)>")
  def eval(i: Int): Row = {
    Row.of(java.lang.String.valueOf(i), java.time.Instant.ofEpochSecond(i))
  }

  // 允许任意类型的符入，并输出定制序列化后的值
  @DataTypeHint(value = "RAW", bridgedTo = classOf[java.nio.ByteBuffer])
  def eval(@DataTypeHint(inputGroup = InputGroup.ANY) o: Any): java.nio.ByteBuffer = {
    BufferUtil.create(Convert.toPrimitiveByteArray(o))
  }
}

