package com.soy.sparkapp.model

/**
  * @author soy
  * @version [版本号, ${date}]
  * @see [相关类/方法]
  * @since [产品/模块版本]
  */
object FieldDataTypes extends Enumeration{
  type FieldDataTypes = Value
  val  StringType, BinaryType, BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, TimestampType= Value
}
