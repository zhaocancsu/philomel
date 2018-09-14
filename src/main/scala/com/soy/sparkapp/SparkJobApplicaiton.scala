package com.soy.sparkapp

import com.soy.sparkapp.common.GlobalVars
import com.soy.sparkapp.message.HttpMsgSend
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication


/**
  * @author  soy
  */
object SparkJobApplication {
  def main(args: Array[String]): Unit = {
    SpringApplication.run(classOf[BootClass],args :_*)

    Runtime.getRuntime.addShutdownHook(new Thread(new DriverShutdownHookThread))
  }
}

class DriverShutdownHookThread  extends Runnable{
  override def run(): Unit = {
    val logger = LoggerFactory.getLogger(classOf[DriverShutdownHookThread])

    val appId: String = GlobalVars.ctx.appId

    if (StringUtils.isNotEmpty(appId)) {
      val url: String = GlobalVars.RESOURCE_COMPONENT_URL + "driverShutdownAction.do"
      logger.info("回调资源中心=>" + url + ",appId=" + appId)
      val params = Seq(("appId", appId))
      try{
        HttpMsgSend.driverShutdown(params)
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }
}
