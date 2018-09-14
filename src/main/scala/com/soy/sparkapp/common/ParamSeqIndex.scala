package com.soy.sparkapp.common

/**
  * author  soy
  * version  [版本号, 2018/9/5]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
object ParamSeqIndex extends Enumeration(0){
  type ParamSeqIndex = Value
  val DriverPort,
  DriverIp,
  ResourceIp,
  ResourcePort,
  JarId,
  AppId,
  ServerId = Value

}
