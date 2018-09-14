package com.soy.sparkapp.common

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import com.soy.sparkapp.model.Context
import org.apache.commons.lang3.StringUtils

/**
  * 解析请求参数
  *
  * author  soy
  * version  [版本号, 2017/2/18]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
object ParseReqParamsUtil {

  /**
    * 通用参数解析
    *
    * @param request http请求
    * @param ctx     计算中心上下文
    */
  def parse(request: HttpServletRequest, ctx: Context): Boolean = {

    val sparkPath = request.getParameter("sparkPath")
    if (StringUtils.isNotEmpty(sparkPath)) {
      ctx.params.sparkMasterUrl = sparkPath
    }
    else {
      ctx.entity.setCode(SysRetCode.ERROR)
      ctx.entity.setDesc("spark master地址为空")
      return false
    }

    val hdfsPathPrefix = request.getParameter("hdfsPath")
    if (StringUtils.isNotEmpty(hdfsPathPrefix)) {
      ctx.params.hdfsPathPrefix = hdfsPathPrefix
    }
    else {
      ctx.entity.setCode(SysRetCode.ERROR)
      ctx.entity.setDesc("hdfs前缀地址为空")
      return false
    }

    val coresNum = Option(request.getParameter("coresNum"))
    ctx.params.coresNum = coresNum.getOrElse("2")

    val executorMemory = Option(request.getParameter("executorMemory"))
    ctx.params.memorySize = executorMemory.getOrElse("512")

    //解析请求生成服务id
    parseServiceId(request, ctx)

    true

  }

  def parseServiceId(request: HttpServletRequest, ctx: Context): Unit = {
    val jobCode = request.getParameter("jobCode")
    val taskCode = request.getParameter("taskCode")
    if (StringUtils.isNotEmpty(jobCode)) {
      if (StringUtils.isNotEmpty(taskCode)) {

        ctx.params.serviceId = jobCode + "-" + taskCode
      }
      else {
        ctx.params.serviceId = jobCode
      }

    }
  }

  def sparkSessionStatus(ctx:Context): Boolean ={
    val sparkCtx = ctx.sparkSession

    if (null == sparkCtx) {
      ctx.entity.setCode(SysRetCode.ERROR)
      ctx.entity.setDesc("SparkSession未创建")
      return false
    }
    if (sparkCtx.sparkContext.isStopped) {
      ctx.entity.setCode(SysRetCode.ERROR)
      ctx.entity.setDesc("SparkSession已停止")
      return false
    }

    true
  }
}
