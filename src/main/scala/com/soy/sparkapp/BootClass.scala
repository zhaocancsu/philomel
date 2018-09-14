package com.soy.sparkapp

import java.lang.management.ManagementFactory

import com.soy.sparkapp.common.{GlobalVars, ParamSeqIndex}
import com.soy.sparkapp.message.HttpMsgSend
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.{EnableAutoConfiguration, SpringBootApplication}
import org.springframework.context.annotation.Bean
import org.springframework.core.env.Environment



/**
  *
  * @author  soy
  */
@SpringBootApplication
@EnableAutoConfiguration
class BootClass {
  val logger = LoggerFactory.getLogger(classOf[BootClass])

  @Autowired
  private val env: Environment = null

  @Bean
  def init(): CommandLineRunner = {
    new CommandLineRunner {
      override def run(args: String*): Unit = {
        val jvmInfo = ManagementFactory.getRuntimeMXBean.getName
        val pid = jvmInfo.split("@")(0)
        if(args.length == ParamSeqIndex.maxId){

          val resourcePath = "http://" + args(ParamSeqIndex.ResourceIp.id) + ":" + args(ParamSeqIndex.ResourcePort.id) + "resource/"
          GlobalVars.RESOURCE_COMPONENT_URL = resourcePath
          val params = Seq("driverPort"->args(ParamSeqIndex.DriverPort.id),"driverIp" -> args(ParamSeqIndex.DriverIp.id),
                           "pid" -> pid,"jarId" -> args(ParamSeqIndex.JarId.id),
                          "appId" -> args(ParamSeqIndex.AppId.id),"serverId" -> args(ParamSeqIndex.ServerId.id))
          HttpMsgSend.appAdd(params)
        }

        logger.info("进程号:"+pid+"端口:"+env.getProperty("server.port")+"->启动成功")

      }
    }
  }

}
