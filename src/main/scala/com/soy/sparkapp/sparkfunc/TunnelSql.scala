package com.soy.sparkapp.sparkfunc

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.model.Context
import org.apache.commons.lang3.StringUtils

/**
  * @author  soy
  */
object TunnelSql extends SparkGadget{

  override def parser(input: HttpServletRequest, ctx: Context): Boolean = {
    val params = ctx.params
    val sql = input.getParameter("sql")
    if (StringUtils.isEmpty(sql)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("SQL语句不能为空")
      return false
    }

    params.sql = sql

    true
  }

  @throws(classOf[Throwable])
  override def execute(ctx: Context): Entity = {
    val resp = ctx.entity

    //执行sql
    val sparkSession = ctx.sparkSession
    val sql = ctx.params.sql
    sparkSession.sparkContext.setJobDescription(sql)
    sparkSession.sql(sql)

    ctx.params.sql = null

    resp
  }
}
