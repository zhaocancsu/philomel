package com.soy.sparkapp.common

import java.net.InetAddress
import java.util.UUID

import com.soy.sparkapp.model.Context
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * author  soy
  * version  [版本号, 2017/2/18]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
object SparkContextUtil {

  //加载jar名称集合
  val jars = Array("mysql-connector-java-5.1.44.jar",
    /*"hbase-client-1.4.2.jar", "hbase-common-1.4.2.jar",
    "hbase-hadoop2-compat-1.4.2.jar", "hbase-hadoop-compat-1.4.2.jar",
    "hbase-metrics-1.4.2.jar", "hbase-metrics-api-1.4.2.jar",
    "hbase-protocol-1.4.2.jar","hbase-server-1.4.2.jar",
    "hive-hbase-handler-2.3.2.jar","philomel-1.0.jar",
    "htrace-core-3.1.0-incubating.jar", "datanucleus-api-jdo-3.2.6.jar",
    "datanucleus-core-3.2.10.jar", "datanucleus-rdbms-3.2.9.jar", "metrics-core-2.2.0.jar",*/
    "phoenix-hive-4.14.0-HBase-1.4.jar","phoenix-core-4.14.0-HBase-1.4.jar",
    "phoenix-4.14.0-HBase-1.4-hive.jar"
    /*"zookeeper-3.4.10.jar","guava-14.0.1.jar"*/
    )
  //"phoenix-4.14.0-HBase-1.4-client.jar","phoenix-4.14.0-HBase-1.4-hive.jar"

  /**
    * spark session初始化
    *
    * @param ctx 计算中心上下文
    */
  def init(ctx: Context): Unit = {

    val params = ctx.params
    //System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop");
    val conf: SparkConf = new SparkConf
    val appName = genAppName(Option(params.serviceId))
    conf.setAppName(appName)
    conf.setMaster(params.sparkMasterUrl)
    conf.set("spark.executor.memory", params.memorySize + "m")
    conf.set("spark.cores.max", params.coresNum)

    //val hdfsJars = jars.map(params.hdfsPathPrefix + "/tmp/sjwj/" + _)
    val localJars = jars.map("/opt/jars/" + _)
    conf.setJars(localJars)

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.scheduler.mode", "FAIR")
    //conf.set("spark.eventLog.dir", ctx.params.hdfsPathPrefix + "/spark/applicationHistory")
    //conf.set("spark.eventLog.enabled", "true")

    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.executor.heartbeatInterval", "600s")
    conf.set("spark.network.timeout", "3600s")
    conf.set("spark.driver.maxResultSize", "8g")
    conf.set("spark.kryoserializer.buffer.max", "1024m")
    conf.set("spark.kryo.referenceTracking", "false")

    //conf.set("spark.driver.userClassPathFirst", "true")
    //conf.set("spark.executor.userClassPathFirst", "true")

    val sparkSession = SparkSession.builder.config(conf)
      .enableHiveSupport.getOrCreate()
    ctx.entity.setAppid(sparkSession.sparkContext.applicationId)
    ctx.entity.setAppname(appName)
    ctx.appId = sparkSession.sparkContext.applicationId
    ctx.appName = appName
    //sparkSession.sparkContext.addFile(ctx.params.hdfsPathPrefix + "/tmp/sjwj/hbase-site.xml")

    ctx.sparkSession = sparkSession
  }

  def genAppName(serviceId: Option[String]): String = {
    val serviceName = serviceId.getOrElse("spark")

    val uuid: String = UUID.randomUUID.toString.replace("-", "")
    val bf: StringBuilder = new StringBuilder
    bf.append(serviceName).append("_").append(InetAddress.getLocalHost.getHostAddress).append("_").append(uuid)
    bf.toString
  }

  /**
    * hive session配置加载
    */
  def initHiveSessionState(): Unit = {
    val hiveConf: HiveConf = new HiveConf(classOf[SessionState])
    SessionState.start(hiveConf)
  }

  /**
    * 停止spark session
    *
    * @param ctx 计算中心上下文
    * @return
    */
  def stop(ctx: Context): Boolean = {
    try {
      if (null != ctx.sparkSession && !ctx.sparkSession.sparkContext.isStopped) {
        ctx.sparkSession.stop()

        ctx.sparkSession = null
      }
    }
    catch {
      case e: Throwable =>
        e.printStackTrace()
        ctx.entity.setErrorStack(ExceptionUtils.getStackTrace(e))
        return false
    }
    true
  }
}
