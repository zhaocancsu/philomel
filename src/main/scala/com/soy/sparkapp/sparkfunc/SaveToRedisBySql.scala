package com.soy.sparkapp.sparkfunc

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.base.SysRetCode
import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.common.JedisConnectionPool
import com.soy.sparkapp.model.{Context, DbConfigParams, JdbcPartitionParams}
import org.apache.commons.lang.math.NumberUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.{Jedis, Pipeline}

import scala.util.Random


/**
  * 根据spark sql计算生成dataframe,再把dataframe存储到redis
  * author  soy
  */
object SaveToRedisBySql extends SparkGadget {

  override def parser(input: HttpServletRequest, ctx: Context): Boolean = {
    val params = ctx.params
    val sql = input.getParameter("sql")
    if (StringUtils.isEmpty(sql)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("SQL语句不能为空")
      return false
    }

    val host = input.getParameter("host")
    if (StringUtils.isEmpty(host)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("redis地址为空")
      return false
    }

    val portStr = Option(input.getParameter("port"))
    val port = portStr.getOrElse("6739")
    if (!NumberUtils.isNumber(port)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("redis端口号非数字")
      return false
    }

    val dbIdxStr = Option(input.getParameter("db_index"))
    val dbIndex = dbIdxStr.getOrElse("0")
    if (!NumberUtils.isNumber(dbIndex)) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("database index非数字")
      return false
    }

    val db = dbIndex.toInt
    if (db > 15 || db < 0) {
      ctx.entity.setCode(SysRetCode.PARAM_INCOMPLETE)
      ctx.entity.setDesc("database index非法,不在0-15范围内")
      return false
    }

    val password = input.getParameter("password")
    if (StringUtils.isNotEmpty(password)) {
      params.password = password
    }

    //redis输出格式
    val formatStr = input.getParameter("storgeType")
    if (StringUtils.isEmpty(formatStr)) {
      params.redisStorgeType = RedisStorageType.HSet
    }
    else {
      params.redisStorgeType = RedisStorageType(formatStr.toInt)
    }

    //数据库连接信息(默认spark sql方式)
    val dbUrl = input.getParameter("dburl")
    if (StringUtils.isNotEmpty(dbUrl)) {
      val dbDriver = input.getParameter("dbdriver")
      if (StringUtils.isNotEmpty(dbDriver) && !StringUtils.equals(dbDriver, "com.chinamobile.cmss.ht.Driver")) {
        val dbUser = input.getParameter("dbuser")
        val dbPassword = input.getParameter("dbpassword")

        val dbConfigParams = DbConfigParams(dbUrl, dbDriver, dbUser, dbPassword)
        params.dbConfigParams = dbConfigParams
      }
    }

    params.sql = sql
    params.host = host
    params.port = port.toInt
    params.dbIndex = db

    val isPipeLine = input.getParameterMap.containsKey("pipeline")
    if (isPipeLine) {
      params.redisPipeLine = true
    }


    val partitions = input.getParameter("partitions")
    if (StringUtils.isNotEmpty(partitions)) {
      val lowerBound = input.getParameter("lowerbound")
      val upperBound = input.getParameter("upperbound")

      val jdbcPartitionParams = JdbcPartitionParams(partitions,lowerBound,upperBound)
      params.jdbcPartitionParams = jdbcPartitionParams
    }

    true
  }

  @throws(classOf[Throwable])
  override def execute(ctx: Context): Entity = {
    val resp = ctx.entity

    //执行sql
    val sparkSession = ctx.sparkSession
    val sql = ctx.params.sql

    var df: DataFrame = null

    val dbConfigParams = ctx.params.dbConfigParams
    if (null != dbConfigParams) {
      val driver = dbConfigParams.driverClassName
      val url = dbConfigParams.connectUrl
      val user = dbConfigParams.userName
      val password = dbConfigParams.password

      val tempTbName = Random.alphanumeric.filter(_.isLetter).take(5).mkString("")
      val dbTable = "(".concat(sql).concat(") ").concat(tempTbName)

      val options = scala.collection.mutable.Map("url" -> url, "driver" -> driver,
        "user" -> user, "password" -> password,
        "dbtable" -> dbTable)

      val jdbcPartitionParams = ctx.params.jdbcPartitionParams
      if(null != jdbcPartitionParams){
        options += ("partitionColumn" -> "seqid")
        options += ("numPartitions" -> jdbcPartitionParams.numPartitions)
        if(StringUtils.isNotEmpty(jdbcPartitionParams.lowerBound)){
          options += ("lowerBound" -> jdbcPartitionParams.lowerBound)
        }
        if(StringUtils.isNotEmpty(jdbcPartitionParams.upperBound)){
          options += ("upperBound" -> jdbcPartitionParams.upperBound)
        }
      }

      sparkSession.sparkContext.setJobDescription(s"db-sql=>$sql")
      df = sparkSession.sqlContext.read.format("jdbc").options(options).load()

    }
    else {
      sparkSession.sparkContext.setJobDescription(sql)
      df = sparkSession.sqlContext.sql(sql)
    }


    var saveFunc: (Array[String], Jedis, Row, String) => Unit = null

    var saveFuncPipeline: (Array[String], Pipeline, Row, Int) => Int = null

    val colNames = df.schema.fieldNames

    val storageType = ctx.params.redisStorgeType

    storageType match {
      case RedisStorageType.String => {
        if (colNames.length < 2) {
          resp.setCode(SysRetCode.ILLEGAL_PARAM)
          resp.setDesc("sql schema列数非法,小于2")
          return resp
        }
        if(ctx.params.redisPipeLine){
          saveFuncPipeline = saveToRedisForStringPL
        }else{
          saveFunc = saveToRedisForString
        }
      }
      case RedisStorageType.HSet => {
        if (colNames.length < 3) {
          resp.setCode(SysRetCode.ILLEGAL_PARAM)
          resp.setDesc("sql schema列数非法,小于3")
          return resp
        }
        if(ctx.params.redisPipeLine){
          saveFuncPipeline = saveToRedisForHSetPL
        }else {
          saveFunc = saveToRedisForHSet
        }
      }

      case RedisStorageType.List => {
        if (colNames.length < 2) {
          resp.setCode(SysRetCode.ILLEGAL_PARAM)
          resp.setDesc("sql schema列数非法,不能小于2")
          return resp
        }
        if(ctx.params.redisPipeLine){
          saveFuncPipeline = saveToRedisForListPL
        }else {
          saveFunc = saveToRedisForList
        }
      }

      case RedisStorageType.Set => {
        if (colNames.length < 2) {
          resp.setCode(SysRetCode.ILLEGAL_PARAM)
          resp.setDesc("sql schema列数非法,不能小于2")
          return resp
        }
        if(ctx.params.redisPipeLine){
          saveFuncPipeline = saveToRedisForSetPL
        }else {
          saveFunc = saveToRedisForSet
        }
      }

      case RedisStorageType.ZSet => {
        if (colNames.length < 3) {
          resp.setCode(SysRetCode.ILLEGAL_PARAM)
          resp.setDesc("sql schema列数非法,不能小于3")
          return resp
        }
        if(ctx.params.redisPipeLine){
          saveFuncPipeline = saveToRedisForSortedSetPL
        }else {
          saveFunc = saveToRedisForSortedSet
        }
      }
    }


    //写入redis
    val ip = ctx.params.host
    val password = if (StringUtils.isEmpty(ctx.params.password)) null.asInstanceOf[String] else ctx.params.password
    val port = ctx.params.port
    val dbIdx = ctx.params.dbIndex

    if(ctx.params.redisPipeLine){
      df.foreachPartition(it => {
        val connection: Jedis = JedisConnectionPool.getConnection(ip, password, port, dbIdx)
        val p = connection.pipelined()
        var counter = 0
        it.foreach(row => {
          counter = saveFuncPipeline(colNames, p, row,counter)

          if(counter > 20000){
            p.sync()
            counter = 0
          }
        })
        if(counter > 0){
          p.sync()
        }
        connection.close()
      })
    }else{
      df.foreachPartition(it => {
        val connection: Jedis = JedisConnectionPool.getConnection(ip, password, port, dbIdx)
        it.foreach(row => {
          saveFunc(colNames, connection, row, null.asInstanceOf[String])
        })
        connection.close()
      })
    }

    resp
  }

  /**
    * 按key-value格式写入redis
    *
    * @param colNames 列名
    * @param connection redis连接
    * @param row 列数据
    */
  private[this] def saveToRedisForSet(colNames: Array[String], connection: Jedis, row: Row, mode: String): Unit = {
    val key = row.get(0)
    val value = row.get(1)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != value && value.toString.trim.nonEmpty) {
        connection.sadd(key.toString, value.toString)
      }
    }
  }

  /**
    * 按key-value格式写入redis
    *
    * @param colNames 列名
    * @param connection redis连接
    * @param row 列数据
    */
  private[this] def saveToRedisForList(colNames: Array[String], connection: Jedis, row: Row, mode: String): Unit = {
    val key = row.get(0)
    val value = row.get(1)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != value && value.toString.trim.nonEmpty) {
        connection.lpush(key.toString, value.toString)
      }
    }
  }

  /**
    * 按key-value格式写入redis
    *
    * @param colNames 列名
    * @param connection redis连接
    * @param row 列数据
    */
  private[this] def saveToRedisForString(colNames: Array[String], connection: Jedis, row: Row, mode: String): Unit = {
    val key = row.get(0)
    val value = row.get(1)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != value && value.toString.trim.nonEmpty) {
        connection.set(key.toString, value.toString)
      }
    }
  }

  /**
    * 按key-field-value格式写入redis
    *
    * @param colNames 列名
    * @param connection redis连接
    * @param row 列数据
    */
  private[this] def saveToRedisForHSet(colNames: Array[String], connection: Jedis, row: Row, mode: String): Unit = {
    val key = row.get(0)
    val field = row.get(1)
    val value = row.get(2)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != field && field.toString.trim.nonEmpty && null != value && value.toString.trim.nonEmpty) {
        connection.hset(key.toString, field.toString, value.toString)
      }

      if (colNames.length > 3) {
        for (i <- 3 until colNames.length) {
          val extraField = colNames(i)
          val extraValue = row.get(i)

          if (null != extraField && extraField.nonEmpty && null != extraValue && extraValue.toString.nonEmpty) {
            connection.hset(key.toString, extraField, extraValue.toString)
          }
        }
      }
    }
  }

  /**
    * SortedSet格式写入redis
    * @param colNames 列名
    * @param connection jedis连接
    * @param row spark df列
    * @param mode
    */
  private[this] def saveToRedisForSortedSet(colNames: Array[String], connection: Jedis, row: Row, mode: String): Unit = {
    val key = row.get(0)
    val score = row.get(1)
    val member = row.get(2)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != score && score.toString.trim.nonEmpty && null != member && member.toString.trim.nonEmpty) {
        connection.zadd(key.toString.trim,score.toString.trim.toDouble,member.toString.trim)
      }
    }
  }

  /**
    * pipeline方式写redis set数据
    * @param colNames 列名
    * @param pipeline pl对象
    * @param row 数据列
    * @param counter 分区计数器
    * @return
    */
  private[this] def saveToRedisForSetPL(colNames: Array[String], pipeline: Pipeline, row: Row,counter: Int): Int = {
    val key = row.get(0)
    val value = row.get(1)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != value && value.toString.trim.nonEmpty) {
        pipeline.sadd(key.toString, value.toString)
        return counter + 1
      }
    }
    counter
  }

  /**
    * pipeline方式写redis List数据
    * @param colNames 列名
    * @param pipeline pl对象
    * @param row 数据列
    * @param counter 分区计数器
    * @return
    */
  private[this] def saveToRedisForListPL(colNames: Array[String], pipeline: Pipeline, row: Row,counter: Int): Int = {
    val key = row.get(0)
    val value = row.get(1)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != value && value.toString.trim.nonEmpty) {
        pipeline.lpush(key.toString, value.toString)
        return counter + 1
      }
    }
    counter
  }

  /**
    * pipeline方式写redis String数据
    * @param colNames 列名
    * @param pipeline pl对象
    * @param row 数据列
    * @param counter 分区计数器
    * @return
    */
  private[this] def saveToRedisForStringPL(colNames: Array[String], pipeline: Pipeline, row: Row,counter: Int): Int = {
    val key = row.get(0)
    val value = row.get(1)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != value && value.toString.trim.nonEmpty) {
        pipeline.set(key.toString, value.toString)
        return counter + 1
      }
    }
    counter
  }

  /**
    * pipeline方式写redis hash数据
    * @param colNames 列名
    * @param pipeline pl对象
    * @param row 数据列
    * @param counter 分区计数器
    * @return
    */
  private[this] def saveToRedisForHSetPL(colNames: Array[String], pipeline: Pipeline, row: Row,counter: Int): Int = {
    val key = row.get(0)
    val field = row.get(1)
    val value = row.get(2)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != field && field.toString.trim.nonEmpty && null != value && value.toString.trim.nonEmpty) {
        pipeline.hset(key.toString, field.toString, value.toString)
      }

      if (colNames.length > 3) {
        for (i <- 3 until colNames.length) {
          val extraField = colNames(i)
          val extraValue = row.get(i)

          if (null != extraField && extraField.nonEmpty && null != extraValue && extraValue.toString.nonEmpty) {
            pipeline.hset(key.toString, extraField, extraValue.toString)
          }
        }
      }
      return counter + 1
    }
    counter
  }

  /**
    * pipeline方式写redis sorted set数据
    * @param colNames 列名
    * @param pipeline pl对象
    * @param row 数据列
    * @param counter 分区计数器
    * @return
    */
  private[this] def saveToRedisForSortedSetPL(colNames: Array[String], pipeline: Pipeline, row: Row,counter: Int): Int = {
    val key = row.get(0)
    val score = row.get(1)
    val member = row.get(2)

    if (null != key && key.toString.trim.nonEmpty) {
      if (null != score && score.toString.trim.nonEmpty && null != member && member.toString.trim.nonEmpty) {
        pipeline.zadd(key.toString.trim,score.toString.trim.toDouble,member.toString.trim)
        return counter + 1
      }
    }
    counter
  }

}

object RedisStorageType extends Enumeration(1) {
  type RedisStorageType = Value
  val String, List, Set, ZSet, HSet = Value
}
