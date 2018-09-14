package com.soy.sparkapp.sparkfunc

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.model.Context
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SaveMode

/**
  * @author  soy
  * version  [版本号, 2017/2/18]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
object SelectToSingleTextFile extends SparkGadget {

  override def parser(input: HttpServletRequest, ctx: Context): Boolean = {
    val sql = input.getParameter("sql")
    if (StringUtils.isEmpty(sql)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("SQL语句不能为空")
      return false
    }

    val targetPath = input.getParameter("targetPath")
    if (StringUtils.isEmpty(targetPath)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("targetPath不能为空")
      return false
    }

    val params = ctx.params

    val delimiter = input.getParameter("delimiter")
    if (StringUtils.isNotEmpty(delimiter)) {
      params.delimiter = delimiter
    }



    params.targetPath = targetPath
    params.sql = sql

    true
  }

  @throws(classOf[Throwable])
  override def execute(ctx: Context): Entity = {
    val resp = ctx.entity

    //执行sql
    val sparkSession = ctx.sparkSession
    val sql = ctx.params.sql
    val targetPath = ctx.params.targetPath
    val delimiter = ctx.params.delimiter

    sparkSession.sparkContext.setJobDescription(sql + "==>File:" + targetPath)

    val df = sparkSession.sql(sql)
    //df.repartition(1).rdd.saveAsTextFile(targetPath)

    //import sparkSession.implicits._
    //val lineDf = df.map(row => row.mkString("\t")).toDF()
    //lineDf.repartition(1).write.mode(SaveMode.ErrorIfExists).option().text(targetPath)
    //df.rdd.map(row => row.mkString("\t")).coalesce(1).saveAsTextFile(targetPath)
    df.coalesce(1).write.mode(SaveMode.ErrorIfExists).format("com.databricks.spark.csv")
      .option("delimiter", delimiter)
      .option("header", "false")
      .option("treatEmptyValuesAsNulls", "true")// No effect.
      .option("nullValue", "")
      .save(targetPath)

    resp
  }
}
