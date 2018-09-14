package com.soy.sparkapp.sparkfunc

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.model.Context
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer



/**
  * 此组件属于业务定制开发
  *
  * @author  soy
  */
object SqlToDataFrame extends SparkGadget {

  override def parser(input: HttpServletRequest, ctx: Context): Boolean = {

    val params = ctx.params

    val sqlListStr: String = input.getParameter("sqlList")
    if (StringUtils.isNotEmpty(sqlListStr)) {
      var sqlList: ListBuffer[String] = ListBuffer[String]()
      val sqlJavaList: java.util.List[String] = com.alibaba.fastjson.JSON.parseObject(sqlListStr, classOf[java.util.List[String]])

      for (i <- 0 until sqlJavaList.size) {
        sqlList += sqlJavaList.get(i)
      }
      params.sqlList = sqlList.toList
    }

    if (params.sqlList.isEmpty) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("SQL语句不能为空")
      return false
    }

    /*if(0 != params.sqlList.size % 2){
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("SQL语句不能配对")
      return false
    }*/

    val tableName = input.getParameter("tableName")
    if (StringUtils.isEmpty(tableName)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("临时表名称不能为空")
      return false
    }

    params.tableName = tableName

    true
  }

  @throws(classOf[Throwable])
  override def execute(ctx: Context): Entity = {
    val resp = ctx.entity

    //执行sql
    val sparkSession = ctx.sparkSession
    val sqlList = ctx.params.sqlList
    val tableName = ctx.params.tableName

    import collection.JavaConversions._

    /*for (i <- 0 until sqlList.size by 2){
      val loadSql: String = sqlList.get(i)

      sparkSession.sparkContext.setJobDescription(loadSql + "==>DataFrame Temp Table:[" + ctx.params.tableName + "]")

      val dataFromJsonRDD = sparkSession.sql(loadSql)

      dataFromJsonRDD.createOrReplaceTempView(tableName)

      val inSql = sqlList.get(i + 1)
      sparkSession.sparkContext.setJobDescription(inSql)
      sparkSession.sql(inSql)
    }*/

    if (sqlList.size > 1) {
      val loadSql: String = sqlList.get(0)

      sparkSession.sparkContext.setJobDescription(loadSql + "==>DataFrame Temp Table:[" + ctx.params.tableName + "]")

      val dataFromJsonRDD = sparkSession.sql(loadSql)

      dataFromJsonRDD.createOrReplaceTempView(tableName)
    }
    val subList = sqlList.subList(1, sqlList.size)

    for (sql <- subList) {
      sparkSession.sparkContext.setJobDescription(sql)
      sparkSession.sql(sql)
    }

    resp
  }
}
