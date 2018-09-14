package com.soy.sparkapp.sparkfunc

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.model.Context
import org.apache.commons.lang3.StringUtils

/**
  * @author  soy
  */
object JsonToDbBySql extends SparkGadget {

  override def parser(input: HttpServletRequest, ctx: Context): Boolean = {

    val sql = input.getParameter("sql")
    if (StringUtils.isEmpty(sql)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("SQL语句不能为空")
      return false
    }

    val jsonStr = input.getParameter("objectJson")
    if (StringUtils.isEmpty(jsonStr)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("JSON字符串不能为空")
      return false
    }

    val tableName = input.getParameter("tableName")
    if (StringUtils.isEmpty(tableName)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("临时表名称不能为空")
      return false
    }

    val params = ctx.params
    params.sql = sql
    params.objectJson = jsonStr
    params.tableName = tableName

    true
  }

  @throws(classOf[Throwable])
  override def execute(ctx: Context): Entity = {
    val resp = ctx.entity

    //执行sql
    val sparkSession = ctx.sparkSession
    val sql = ctx.params.sql
    val jsonStr = ctx.params.objectJson
    val tableName = ctx.params.tableName

    sparkSession.sparkContext.setJobDescription(sql + "==>temp table:" + tableName)

    val jRdd = sparkSession.sparkContext.makeRDD(jsonStr :: Nil)
    val dataFromJsonRDD = sparkSession.read.json(jRdd)
    dataFromJsonRDD.createOrReplaceTempView(tableName)
    sparkSession.sql(sql)

    resp
  }
}
