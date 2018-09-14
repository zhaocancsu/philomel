package com.soy.sparkapp.sparkfunc

import javax.servlet.http.HttpServletRequest

import cn.migu.unify.comm.resp.resource.Entity
import com.soy.sparkapp.model.Context

/**
  * @author  soy
  */
trait SparkGadget {

  def parser(input: HttpServletRequest, ctx: Context): Boolean

  def execute(ctx: Context): Entity
  
}
