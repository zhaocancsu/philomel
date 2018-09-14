package com.soy.sparkapp.config

import javax.annotation.PostConstruct

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.json.PackageVersion
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonSerializer, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationListener
import org.springframework.context.annotation.{Bean, ComponentScan, Configuration, Scope}
import org.springframework.web.context.request.async.DeferredResult
import springfox.documentation.builders.{ApiInfoBuilder, ParameterBuilder, RequestHandlerSelectors}
import springfox.documentation.schema.ModelRef
import springfox.documentation.schema.configuration.ObjectMapperConfigured
import springfox.documentation.service.{ApiInfo, Contact, Parameter}
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2

import scala.collection.mutable.ListBuffer

@Configuration
@EnableSwagger2
@ComponentScan(Array("com.soy.sparkapp.controller"))
class SwaggerConfig {

  @Value("${swagger.ui.enable}")
  val swagger_ui_enable: Boolean = false

  /**
    * 用户接口列表
    *
    * @return
    */
  @Bean
  def userDocketFactory: Docket = {
    val docket = new Docket(DocumentationType.SWAGGER_2)
      .apiInfo(apiInfo)
      .groupName("sparkJobInterface")
      .select()
      .apis(RequestHandlerSelectors.basePackage("com.soy.sparkapp.controller"))
      .build()
      //.globalOperationParameters(commonParameters)
      .useDefaultResponseMessages(false)
    docket.enable(swagger_ui_enable)
  }

  def apiInfo: ApiInfo = {
    new ApiInfoBuilder()
      .title("Spark Job,Spring Boot")
      .description("Spark Application Server Api")
      .contact(new Contact("soy","","zhaocan@migu.cn"))
      .license("Apache License, Version 2.0")
      .version("1.0")
      .build()
  }

  def commonParameters: java.util.List[Parameter]={
    val parameters = new ListBuffer[Parameter]
    parameters += new ParameterBuilder()
      .name("batchNo")
      .description("提交标识id")
      .modelRef(new ModelRef("string"))
      .parameterType("form")
      .required(false)
      .build()
    parameters += new ParameterBuilder()
      .name("jobCode")
      .description("调度任务编码")
      .modelRef(new ModelRef("string"))
      .parameterType("form")
      .required(false)
      .build()
    parameters += new ParameterBuilder()
      .name("taskCode")
      .description("调度任务中task编码")
      .modelRef(new ModelRef("string"))
      .parameterType("form")
      .required(false)
      .build()
    parameters += new ParameterBuilder()
      .name("coresNum")
      .description("资源总核数,默认2")
      .modelRef(new ModelRef("string"))
      .parameterType("form")
      .required(false)
      .build()

    parameters += new ParameterBuilder()
      .name("executorMemory")
      .description("每个executor启动内存大小(单位M),默认512m")
      .modelRef(new ModelRef("string"))
      .parameterType("form")
      .required(false)
      .build()
    parameters += new ParameterBuilder()
      .name("sparkPath")
      .description("spark master地址,例如,spark://master:7077")
      .modelRef(new ModelRef("string"))
      .parameterType("form")
      .required(true)
      .build()
    parameters += new ParameterBuilder()
      .name("hdfsPath")
      .description("hdfs地址schema,例如,hdfs://master:9000")
      .modelRef(new ModelRef("string"))
      .parameterType("form")
      .required(true)
      .build()

    import scala.collection.JavaConversions._
    parameters
  }

  @PostConstruct
  def initObject(): Unit = {
    //    objectMapper.registerModule(DefaultScalaModule)
  }

  /**
    * 如果采用注入objectMapper，然后在initObject中添加对scala的处理，那样在帮助文档中至少目前不会显示@ApiModelProperty的注解，
    * 采用事件触发添加会显示，但测试下来发现只显示Response的，如果PostBody则又会忽略，感觉这块跟scala结合的还不是很好，
    * 也有可能跟scala的类定义有关，还需仔细看下
    *
    * @return
    */
  @Bean
  def objectMapperInitializer = new ApplicationListener[ObjectMapperConfigured] {
    def onApplicationEvent(event: ObjectMapperConfigured) = {
      event.getObjectMapper.registerModule(DefaultScalaModule)

      val newModule = new SimpleModule("UTCDateDeserializer", PackageVersion.VERSION)
      newModule.addSerializer(classOf[DeferredResult[_]], new DeferredResultSerializer(event.getObjectMapper))
      event.getObjectMapper.registerModule(newModule)
    }
  }
}

class DeferredResultSerializer(objectMapper: ObjectMapper) extends JsonSerializer[DeferredResult[_]] {

  override def serialize(value: DeferredResult[_], gen: JsonGenerator, serializers: SerializerProvider) = {
    gen.writeString(objectMapper.writeValueAsString(value.getResult))
  }
}
