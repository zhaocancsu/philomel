package com.soy.sparkapp.parttern

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.model.Context
import com.soy.sparkapp.sparkfunc.SparkGadget

/**
  * author  soy
  * version  [版本号, 2017/2/19]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
object Entrance {
  def call[A <: SparkGadget](request: HttpServletRequest, ctx: Context, handler: A): Entity = {
    val initStatus = Backbone.init(request, ctx)(handler.parser)
    if (!initStatus) {
      return ctx.entity
    }

    val entity = Backbone.using(ctx)(handler.execute)
    entity
  }

  def blockCall[A <: SparkGadget](request: HttpServletRequest, ctx: Context, handler: A): Entity = {
    val initStatus = Backbone.init(request, ctx)(handler.parser)
    if (!initStatus) {
      return ctx.entity
    }

    val entity = Backbone.blockUsing(ctx)(handler.execute)
    entity
  }
}
