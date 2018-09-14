package com.soy.sparkapp.sparkfunc

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.model.Context
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

/**
  * author  soy
  */
object MultipleSql extends SparkGadget{

  override def parser(input: HttpServletRequest, ctx: Context): Boolean = {
    val params = ctx.params
    val sqlListStr: String = input.getParameter("sqlList")
    if (StringUtils.isNotEmpty(sqlListStr)) {
      var sqlList: ListBuffer[String] = ListBuffer[String]()
      val sqlJavaList: java.util.List[String] = com.alibaba.fastjson.JSON.parseObject(sqlListStr, classOf[java.util.List[String]])
      for (i <- 0 until sqlJavaList.size()) {
        sqlList += sqlJavaList.get(i)
      }
      params.sqlList = sqlList.toList
    }

    if (params.sqlList.isEmpty) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("SQL语句不能为空")
      return false
    }

    true
  }

  @throws(classOf[Throwable])
  override def execute(ctx: Context): Entity = {
    val resp = new Entity
    resp.setAppid(ctx.appId)
    resp.setAppname(ctx.appName)

    //执行sql
    val sparkSession = ctx.sparkSession
    val sqlList = ctx.params.sqlList

    for (sql <- sqlList) {
      sparkSession.sparkContext.setJobDescription(sql)
      sparkSession.sql(sql)
    }

    resp
  }
}
