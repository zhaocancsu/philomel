package com.soy.sparkapp.message

import java.net.InetAddress

import com.soy.sparkapp.common.GlobalVars
import com.soy.sparkapp.model.Context
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.Map
import scalaj.http.Http

/**
  * 业务流程消息发送
  * author  soy
  * version  [版本号, 2017/2/20]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
object HttpMsgSend {

  private val errorLog: Log = LogFactory.getLog("error")

  val SPARK_APP_SUBMIT_MSG = "sparkAppSubmit.do"

  val SPARK_APP_RUN_FINISH_MSG = "backBySparkAppEnd.do"

  val SPARK_APP_ADD_MSG = "insertJob.do"

  val SPARK_DRIVER_SHUTDOWN_MSG = "driverShutdownAction.do"

  def appAdd(params: Seq[(String,String)]) = {
    Http(GlobalVars.RESOURCE_COMPONENT_URL + SPARK_APP_ADD_MSG).postForm(params).asString
  }

  def driverShutdown(params: Seq[(String,String)]) = {
    Http(GlobalVars.RESOURCE_COMPONENT_URL + SPARK_DRIVER_SHUTDOWN_MSG).postForm(params).asString
  }

  /**
    * spark任务提交创建spark context后向资源中心发送消息
    *
    * @param ctx
    */
  def postSubmit(ctx: Context): Unit = {

    if (StringUtils.isEmpty(GlobalVars.RESOURCE_COMPONENT_URL)) return

    try {
      val appName = ctx.appName

      val sql = ctx.params.sql
      val sqlList = ctx.params.sqlList

      val params: Map[String,String] = Map("appId" -> ctx.appId, "appName" -> appName,
        "driverIp" -> InetAddress.getLocalHost.getHostAddress,
        "driverPort" -> GlobalVars.DRIVER_PORT.toString)
      if (StringUtils.isNotEmpty(sql)) {
        params.put("note",sql)
      }
      else if (sqlList != null && sqlList.size > 0) {
        params.put("note", sqlList.mkString)
      }

      //HttpPostUtil.post(GlobalVars.RESOURCE_COMPONENT_URL + SAPRK_APP_SUBMIT_MSG, JavaConversions.mapAsJavaMap(params))
      Http(GlobalVars.RESOURCE_COMPONENT_URL + SPARK_APP_SUBMIT_MSG).postForm(params.toSeq).asString
    }
    catch {
      case e: Throwable => {
        errorLog.error(ExceptionUtils.getStackTrace(e))
      }
    }
  }

  /**
    * spark任务执行结束后向资源中心发送消息
    *
    * @param ctx
    */
  def postFinish(ctx: Context): Unit = {

    if (StringUtils.isEmpty(GlobalVars.RESOURCE_COMPONENT_URL)) return

    try {
      val appName =  ctx.appName
      val params: Map[String,String] = Map("appName" -> appName, "status" -> ctx.entity.getCode,
        "ip" -> InetAddress.getLocalHost.getHostAddress,
        "port" -> GlobalVars.DRIVER_PORT.toString)

      if (StringUtils.isNotEmpty(ctx.entity.getErrorStack)){
        params.put("errorStack", ctx.entity.getErrorStack)
      }

       Http(GlobalVars.RESOURCE_COMPONENT_URL + SPARK_APP_RUN_FINISH_MSG).postForm(params.toSeq).asString
    }
    catch {
      case e: Throwable => {
        errorLog.error(ExceptionUtils.getStackTrace(e))
      }
    }
  }

}
