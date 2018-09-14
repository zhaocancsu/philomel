package com.soy.sparkapp.sparkfunc

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.Entity
import com.google.common.reflect
import com.google.gson.Gson
import com.soy.sparkapp.model.FieldDataTypes.FieldDataTypes
import com.soy.sparkapp.model.{Context, FieldDataTypes, FieldStruct, ServiceParams}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, _}

import scala.collection.{JavaConversions, mutable}

/**
  * author  soy
  */
object LoadHdfsToDataFrame extends SparkGadget {

  override def parser(input: HttpServletRequest, ctx: Context): Boolean = {

    val filePath = input.getParameter("filePath")
    if (StringUtils.isEmpty(filePath)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("filePath不能为空")
      return false
    }

    val sql = input.getParameter("sql")
    if (StringUtils.isEmpty(sql)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("SQL语句不能为空")
      return false
    }

    val tableName = input.getParameter("tableName")
    if (StringUtils.isEmpty(tableName)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("临时表名称tableName不能为空")
      return false
    }

    val fields = input.getParameter("fields")
    if (StringUtils.isNotEmpty(fields)) ctx.params.fields = getField(fields)

    val fieldsList = input.getParameter("fieldsList")
    if (StringUtils.isNotEmpty(fieldsList)) ctx.params.fieldsList = getFieldList(fieldsList)


    val params = ctx.params
    params.sql = sql
    params.filePath = filePath
    params.tableName = tableName

    true
  }

  @throws(classOf[Throwable])
  override def execute(ctx: Context): Entity = {
    val resp = ctx.entity

    //执行sql
    val sparkSession = ctx.sparkSession
    val sql = ctx.params.sql
    val filePath = ctx.params.filePath
    val tableName = ctx.params.tableName

    sparkSession.sparkContext.setJobDescription(sql + "==>load " + filePath + " to temp table:" + tableName)

    val rdd = sparkSession.read.textFile(filePath).rdd.map(line => RowFactory.create(line.split(ctx.params.splitChar)))

    val dataFrame = getDataFrameFromJavaRDD(ctx.params, rdd, sparkSession)
    dataFrame.createOrReplaceTempView(tableName)
    sparkSession.sql(sql)

    resp
  }

  private[this] def getDataFrameFromJavaRDD(params: ServiceParams, rdd: RDD[Row], sparkSession: SparkSession): DataFrame = {
    val fields: mutable.Map[String, FieldDataTypes] = params.fields
    var fieldList: mutable.ListBuffer[StructField] = mutable.ListBuffer[StructField]()
    if (fields != null) {
      fieldList = structFieldByFieldMap(fields)
    }
    else {
      fieldList = structFieldByFieldList(params.fieldsList)
    }
    val ls = JavaConversions.bufferAsJavaList(fieldList)
    val schema: StructType = DataTypes.createStructType(ls)
    val frame: DataFrame = sparkSession.createDataFrame(rdd, schema)
    frame
  }

  private[this] def structFieldByFieldList(fieldsList: List[FieldStruct]): mutable.ListBuffer[StructField] = {
    val fieldList = mutable.ListBuffer[StructField]()
    for (fieldStruct <- fieldsList) {
      val fieldType: DataType = getFieldType(getFieldType(fieldStruct.fieldTypeStr))
      fieldList += DataTypes.createStructField(fieldStruct.fieldName, fieldType, true)
    }
    fieldList
  }

  private[this] def structFieldByFieldMap(fields: mutable.Map[String, FieldDataTypes]): mutable.ListBuffer[StructField] = {
    val fieldList = mutable.ListBuffer[StructField]()
    fields.foreach(e => {
      val (k, v) = e
      val fieldName: String = k
      val fieldType: DataType = getFieldType(v)
      fieldList += DataTypes.createStructField(fieldName, fieldType, true)
    })
    fieldList
  }

  private[this] def getField(fieldsStr: String): mutable.Map[String, FieldDataTypes] = {
    var fields = mutable.Map[String, FieldDataTypes]()
    val gson: Gson = new Gson
    val map: Map[String, String] = gson.fromJson(fieldsStr, classOf[Map[String, String]])
    map.foreach(e => {
      val (k, v) = e
      val fieldName: String = k
      val fieldType: FieldDataTypes = getFieldType(v)
      fields += (fieldName -> fieldType)
    })

    fields
  }

  private[this] def getFieldList(listJson: String): List[FieldStruct] = {
    val gson: Gson = new Gson
    val lists: java.util.List[FieldStruct] = gson.fromJson(listJson, new reflect.TypeToken[java.util.List[FieldStruct]]() {
    }.getType)
    var list: mutable.ListBuffer[FieldStruct] = mutable.ListBuffer[FieldStruct]()
    if (lists != null) {
      for (i <- 0 until lists.size()) {
        list += lists.get(i)
      }
    }
    list.toList
  }

  private[this] def getFieldType(type1: FieldDataTypes): DataType = type1 match {
    case FieldDataTypes.StringType =>
      DataTypes.StringType
    case FieldDataTypes.BinaryType =>
      DataTypes.BinaryType
    case FieldDataTypes.BooleanType =>
      DataTypes.BooleanType
    case FieldDataTypes.ByteType =>
      DataTypes.ByteType
    case FieldDataTypes.DateType =>
      DataTypes.DateType
    case FieldDataTypes.DoubleType =>
      DataTypes.DoubleType
    case FieldDataTypes.FloatType =>
      DataTypes.FloatType
    case FieldDataTypes.IntegerType =>
      DataTypes.IntegerType
    case FieldDataTypes.LongType =>
      DataTypes.LongType
    case FieldDataTypes.NullType =>
      DataTypes.NullType
    case FieldDataTypes.ShortType =>
      DataTypes.ShortType
    case FieldDataTypes.TimestampType =>
      DataTypes.TimestampType
    case _ =>
      DataTypes.StringType
  }

  private[this] def getFieldType(type1: String): FieldDataTypes = type1 match {
    case "StringType" =>
      FieldDataTypes.StringType
    case "BinaryType" =>
      FieldDataTypes.BinaryType
    case "BooleanType" =>
      FieldDataTypes.BooleanType
    case "ByteType" =>
      FieldDataTypes.ByteType
    case "DateType" =>
      FieldDataTypes.DateType
    case "DoubleType" =>
      FieldDataTypes.DoubleType
    case "FloatType" =>
      FieldDataTypes.FloatType
    case "IntegerType" =>
      FieldDataTypes.IntegerType
    case "LongType" =>
      FieldDataTypes.LongType
    case "NullType" =>
      FieldDataTypes.NullType
    case "ShortType" =>
      FieldDataTypes.ShortType
    case "TimestampType" =>
      FieldDataTypes.TimestampType
    case _ =>
      FieldDataTypes.StringType
  }
}
