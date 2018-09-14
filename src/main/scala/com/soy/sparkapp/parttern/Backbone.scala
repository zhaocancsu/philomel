package com.soy.sparkapp.parttern

import java.util.concurrent.Executors
import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.common.{ParseReqParamsUtil, SparkContextUtil}
import com.soy.sparkapp.message.HttpMsgSend
import com.soy.sparkapp.model.Context
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.commons.logging.{Log, LogFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * author  soy
  * version  [版本号, 2017/2/18]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */

object Backbone extends Backbone

trait Backbone {

  private val errorLog: Log = LogFactory.getLog("error")

  def init(params: HttpServletRequest, ctx: Context)(func: (HttpServletRequest, Context) => Boolean): Boolean = {

    //恢复返回信息
    ctx.entity.setCode(SysRetCode.SUCCESS)
    ctx.entity.setDesc("操作成功")
    ctx.entity.setContent(null)
    ctx.entity.setErrorStack(null)

    //非隧道模式需要系统参数
    if (!ctx.tunnelMode) {
      //系统参数解析校验
      if (!ParseReqParamsUtil.parse(params, ctx)) return false
    }
    else {
      //隧道模式校验spark session状态
      if (!ParseReqParamsUtil.sparkSessionStatus(ctx)) return false
    }

    //业务参数校验
    if (null != func) {
      if (!func(params, ctx)) {
        return false
      }
    }

    try {
      //隧道模式提交任务不需要初始化
      if (!ctx.tunnelMode) {
        //初始化spark session
        SparkContextUtil.init(ctx)
      }
      else {
        //隧道模式下的appName
        ctx.appName = SparkContextUtil.genAppName(Option(ctx.params.serviceId))
        ctx.entity.setAppname(ctx.appName)
      }
    } catch {
      case e: Throwable => {
        val errMsg = ExceptionUtils.getStackTrace(e)
        errorLog.error(errMsg)

        ctx.entity.setCode(SysRetCode.SPARK_APP_CREATED_FAILED)
        ctx.entity.setDesc("spark 任务创建失败")
        ctx.entity.setErrorStack(errMsg)
        return false
      }
    }

    //初始化隧道时不反馈消息,设置driver环境模式为隧道模式
    if (!ctx.nonSendSubmitMsg) {
      //发送提交成功消息
      HttpMsgSend.postSubmit(ctx)

    }
    else {
      ctx.nonSendSubmitMsg = false
    }

    true
  }

  private implicit val ec = new ExecutionContext {
    val threadPool = Executors.newFixedThreadPool(1)

    def execute(runnable: Runnable) {
      threadPool.submit(runnable)
    }

    def reportFailure(t: Throwable) {
      throw t
    }
  }

  def using(ctx: Context)(func: Context => Entity): Entity = {
    //val entity = ctx.entity


    val f: Future[Entity] = Future {
      try{
        func(ctx)
      }catch{
        case e: Throwable =>
          e.printStackTrace()
          throw e
      }

    }

    f.onComplete {
      case Success(resp) => {
        resp.setCode(SysRetCode.SPARK_APP_FINISHED)
        ctx.entity = resp

        appRunOver(ctx)
      }
      case Failure(e) => {
        e.printStackTrace()
        //日志
        val errMsg = ExceptionUtils.getStackTrace(e)
        errorLog.error(errMsg)

        val entity = ctx.entity
        entity.setCode(SysRetCode.SPARK_APP_FAILED)
        entity.setErrorStack(ExceptionUtils.getStackTrace(e))

        appRunOver(ctx)
      }
    }
    ctx.entity


  }

  /**
    * 阻塞方法提交spark任务
    *
    * @param ctx  上下文
    * @param func 方法
    * @return
    */
  def blockUsing(ctx: Context)(func: Context => Entity): Entity = {

    try {
      //SparkContextUtil.initHiveSessionState()
      func(ctx)
    } catch {
      case e: Throwable => {

        val errMsg = ExceptionUtils.getStackTrace(e)
        errorLog.error(errMsg)

        ctx.entity.setCode(SysRetCode.SPARK_APP_FAILED)
        ctx.entity.setDesc("spark任务运行失败")
        ctx.entity.setErrorStack(errMsg)
      }
    }

    //如需关闭sparkcontext,则关闭
    //非隧道模式,用完即关闭
    if (!ctx.tunnelMode) {
      SparkContextUtil.stop(ctx)
    }

    //发送spark任务完成请求
    HttpMsgSend.postFinish(ctx)

    ctx.entity

  }

  private[this] def appRunOver(ctx: Context): Unit = {
    //如需关闭sparkcontext,则关闭
    //非隧道模式,用完即关闭
    if (!ctx.tunnelMode) {
      SparkContextUtil.stop(ctx)
    }

    //发送spark任务完成请求
    HttpMsgSend.postFinish(ctx)
  }
}
