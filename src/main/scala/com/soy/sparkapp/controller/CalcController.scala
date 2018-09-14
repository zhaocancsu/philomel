package com.soy.sparkapp.controller

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.{Entity, Response}
import com.soy.sparkapp.common.{GlobalVars, SparkContextUtil}
import com.soy.sparkapp.parttern.{Backbone, Entrance}
import com.soy.sparkapp.sparkfunc._
import io.swagger.annotations._
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.commons.lang3.StringUtils
import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, ResponseBody, RestController}

/**
  * author  soy
  * version  [版本号, 2017/2/18]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
@RestController
@RequestMapping(Array("/"))
@Api("Spark SQL计算接口")
class CalcController {

  @ApiOperation(value = "执行单条spark sql语句",notes = "执行一条spark sql")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sql", value = "要执行的sql语句", required = true, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/executeSql"),method = Array(RequestMethod.POST))
  @ResponseBody def executeSql(request: HttpServletRequest): Response = {

    val resp = new Response

    val entity = Entrance.call(request, GlobalVars.ctx, SingleSql)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "提交执行多条spark sql语句",notes = "提交执行多条spark sql语句")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sqlList", value = "语句列表,json array字符串,例如:[\"sql1\",\"sql2\"]", required = true, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/executeSqlList"),method = Array(RequestMethod.POST))
  @ResponseBody def executeSqlList(request: HttpServletRequest): Response = {
    val resp = new Response

    val entity = Entrance.call(request, GlobalVars.ctx, MultipleSql)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "执行一条spark sql查询语句并将数据保存到hdfs上的json文件中(多个)",notes = "spark sql->json文件")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sql", value = "sql查询语句", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "targetPath", value = "hdfs目标路径", required = true, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/selectToFile"),method = Array(RequestMethod.POST))
  @ResponseBody def selectToFile(request: HttpServletRequest): Response = {
    val resp = new Response

    val entity = Entrance.call(request, GlobalVars.ctx, SelectToFile)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "执行一条spark sql查询语句并将数据保存到hdfs上的一个文本文件中",notes = "spark sql->txt文件")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sql", value = "sql查询语句", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "targetPath", value = "hdfs目标路径", required = true, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/selectToSingleTextFile"),method = Array(RequestMethod.POST))
  @ResponseBody def selectToSingleTextFile(request: HttpServletRequest): Response = {
    val resp = new Response

    val entity = Entrance.call(request, GlobalVars.ctx, SelectToSingleTextFile)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "先将json数据字符串生成临时表,再执行spark sql语句",notes = "json data->temp table->spark sql")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sql", value = "sql查询语句", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "objectJson", value = "json数据字符串", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "tableName", value = "临时表名", required = true, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/jsonToDbBySql"),method = Array(RequestMethod.POST))
  @ResponseBody def jsonToDbBySql(request: HttpServletRequest): Response = {
    val resp = new Response

    val entity = Entrance.call(request, GlobalVars.ctx, JsonToDbBySql)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "第1条sql用于创建临时表,后续sql执行计算",notes = "first sql->dataframe->the other sqls calculate")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "tableName", value = "临时表名", required = true, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/executeSqldataFrame"),method = Array(RequestMethod.POST))
  @ResponseBody def executeSqldataFrame(request: HttpServletRequest): Response = {
    val resp = new Response

    val entity = Entrance.call(request, GlobalVars.ctx, SqlToDataFrame)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "读取hdfs文件数据加载为内存表,然后执行sql",notes = "read file data->create temp table->execute sql")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "filePath", value = "hdfs文件路径", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sql", value = "sql语句", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "tableName", value = "临时表名", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "fields", value = "属性字段",example="json字符串([{'列名1':'类型1','列名2':'类型2'}])", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "fieldsList", value = "属性字段列表",example="json字符串,([{'fieldName':'xx','fieldTypeStr':'yy','fieldType':'zz'}])", required = false, dataType = "String", paramType = "form")

  ))
  @RequestMapping(value = Array("/SparkSQL/dataFromHdfs"),method = Array(RequestMethod.POST))
  @ResponseBody def dataFromHdfs(request: HttpServletRequest): Response = {
    val resp = new Response

    val entity = Entrance.call(request, GlobalVars.ctx, LoadHdfsToDataFrame)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "启动一个spark application运行环境",notes = "初始化spark application运行环境")
  @RequestMapping(value = Array("/SparkSQL/startSparkSession"),method = Array(RequestMethod.POST))
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form")
  ))
  @ResponseBody def startSparkSession(request: HttpServletRequest): Response = {
    val resp = new Response

    GlobalVars.ctx.nonSendSubmitMsg = true

    Backbone.init(request, GlobalVars.ctx)(null)

    if(StringUtils.equals(SysRetCode.SUCCESS,GlobalVars.ctx.entity.getCode)){
      GlobalVars.ctx.tunnelMode = true
    }
    else{
      GlobalVars.ctx.nonSendSubmitMsg = false
    }

    resp.setResponse(GlobalVars.ctx.entity)

    resp
  }

  @ApiOperation(value = "根据application id停止spark application运行环境",notes = "停止spark application运行环境")
  @RequestMapping(value = Array("/SparkSQL/stopSparkSession"),method = Array(RequestMethod.POST))
  @ResponseBody def stopSparkSession(request: HttpServletRequest): Response = {

    val sparkSession = GlobalVars.ctx.sparkSession

    val response = new Response
    val entity = new Entity

    if (null == sparkSession) {
      entity.setCode(SysRetCode.SPARK_APP_FAILED)
      entity.setDesc("SparkSession没初始化")
      response.setResponse(entity)
      return response
    }
    try {
      sparkSession.stop()

      GlobalVars.ctx.tunnelMode = false
      GlobalVars.ctx.sparkSession = null
    } catch {
      case e: Throwable =>
        entity.setCode(SysRetCode.SPARK_APP_FAILED)
        entity.setDesc("SparkSession停止失败")
        entity.setErrorStack(ExceptionUtils.getStackTrace(e))
    }

    response.setResponse(entity)
    response
  }

  @ApiOperation(value = "在已启动的spark运行环境中执行一条sql",notes = "在已启动的spark运行环境中执行一条sql")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "sql", value = "要执行的sql语句", required = true, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/doSessionSql"),method = Array(RequestMethod.POST))
  @ResponseBody def doSessionSql(request: HttpServletRequest): Response = {

    val resp = new Response

    val entity = Entrance.call(request, GlobalVars.ctx, TunnelSql)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "在已启动的spark运行环境中执行一条select sql",notes = "在已启动的spark运行环境中执行一条select sql")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "sql", value = "要执行的查询sql语句", required = true, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/doSessionSelectSql"),method = Array(RequestMethod.POST))
  @ResponseBody def selectSql(request: HttpServletRequest): Response = {

    val resp = new Response

    GlobalVars.ctx.nonSendSubmitMsg = true

    val entity = Entrance.blockCall(request, GlobalVars.ctx, SelectSql)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "spark sql执行计划",notes = "spark sql执行计划")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sql", value = "sql语句", required = true, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/validateSql"),method = Array(RequestMethod.POST))
  @ResponseBody def validateSql(request: HttpServletRequest): Response = {
    val resp = new Response

    GlobalVars.ctx.nonSendSubmitMsg = true

    val entity = Entrance.blockCall(request, GlobalVars.ctx, ValidateSql)

    resp.setResponse(entity)

    resp
  }

  @ApiOperation(value = "停止spark application运行环境",notes = "停止spark application运行环境")
  @RequestMapping(value = Array("/stopSparkAppOnSub"),method = Array(RequestMethod.POST))
  @ResponseBody  def stopSparkContext(request: HttpServletRequest):Response= {

    val response = new Response

    val entity = new Entity

    val status = SparkContextUtil.stop(GlobalVars.ctx)
    if(!status){
      entity.setCode(SysRetCode.ERROR)
      entity.setDesc("停止spark context错误")
      entity.setErrorStack(GlobalVars.ctx.entity.getErrorStack)
      GlobalVars.ctx.entity.setErrorStack(null)
    }

    entity.setAppid(GlobalVars.ctx.entity.getAppid)
    entity.setAppname(GlobalVars.ctx.entity.getAppname)

    response.setResponse(entity)
    response
  }

  @ApiOperation(value = "查询sql数据并输出到redis中(仅支持单节点环境)",notes = "查询sql数据并输出到redis中")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "batchNo", value = "提交标识id", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "jobCode", value = "调度任务编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "taskCode", value = "调度任务中task编码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "coresNum", value = "资源总核数", required = false,defaultValue = "2", dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "executorMemory", value = "每个executor启动内存大小(单位M)",defaultValue="512", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sparkPath", value = "spark master地址",example = "spark://master:7077", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "hdfsPath", value = "hdfs地址schema",example = "hdfs://master:9000", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "sql", value = "sql语句", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "host", value = "redis地址ip", required = true, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "port", value = "端口号",defaultValue="6379", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "db_index", value = "redis库编号",defaultValue="0", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "password", value = "密码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "storgeType", value = "redis存储数据结构(1:String,2:List,3:Set,4:ZSet,5:HSet)",defaultValue="5", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "dburl", value = "数据库连接地址", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "dbuser", value = "数据库连接用户名", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "dbpassword", value = "数据库连接密码", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "partitions", value = "分区数量", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "lowerbound", value = "最小数据位置", required = false, dataType = "String", paramType = "form"),
    new ApiImplicitParam(name = "upperbound", value = "最大数据位置", required = false, dataType = "String", paramType = "form")
  ))
  @RequestMapping(value = Array("/SparkSQL/saveToRedisBySql"),method = Array(RequestMethod.POST))
  @ResponseBody def saveToRedisBySql(request: HttpServletRequest): Response = {
    val resp = new Response

    val entity = Entrance.call(request, GlobalVars.ctx, SaveToRedisBySql)

    resp.setResponse(entity)

    resp
  }


}
