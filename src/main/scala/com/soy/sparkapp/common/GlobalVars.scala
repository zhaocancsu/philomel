package com.soy.sparkapp.common

import com.soy.sparkapp.model.Context

/**
  * author  soy
  * version  [版本号, 2017/2/20]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
object GlobalVars {

  //计算中心全局上下文
  val ctx: Context = new Context

  //资源中心地址
  var RESOURCE_COMPONENT_URL : String = _

  //计算中心端口
  var DRIVER_PORT : Int = _

}
