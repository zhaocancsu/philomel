package com.soy.sparkapp.sparkfunc

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.model.Context
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SaveMode

/**
  * @author  soy
  */
object SelectToFile extends SparkGadget{

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
    sparkSession.sparkContext.setJobDescription(sql)
    sparkSession.sql(sql)

    sparkSession.sparkContext.setJobDescription(sql + "==>File:" + targetPath)
    val rdd = sparkSession.sql(sql)
    rdd.write.mode(SaveMode.ErrorIfExists).json(targetPath)

    resp
  }
}
