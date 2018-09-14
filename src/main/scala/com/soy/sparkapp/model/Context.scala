package com.soy.sparkapp.model

import cn.migu.unify.comm.resp.resource.Entity
import org.apache.spark.sql.SparkSession

/**
  * author  soy
  * version  [版本号, 2017/2/18]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
class Context {
  var params: ServiceParams = new ServiceParams

  var sparkSession: SparkSession = _

  var appName: String = _

  var appId: String = _

  var nonSendSubmitMsg: Boolean = false

  var tunnelMode: Boolean = false

  var entity: Entity = new Entity
}
