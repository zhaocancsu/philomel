package com.soy.sparkapp.common

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * author  soy
  * version  [版本号, 2017/2/23]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
object JedisConnectionPool extends Serializable {
  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(50)
  //最大空闲连接数,
  config.setMaxIdle(5)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  config.setMinIdle(10)
  config.setMaxWaitMillis(3 * 1000)
  config.setTestOnReturn(true)
  config.setTestWhileIdle(true)
  config.setMinEvictableIdleTimeMillis(500)
  config.setSoftMinEvictableIdleTimeMillis(1000)
  config.setTimeBetweenEvictionRunsMillis(1000)
  config.setNumTestsPerEvictionRun(100)

  var pool: JedisPool = _

  def getConnection(ip: String, port: Int, db: Int): Jedis = {
    if(null == pool){
      pool = new JedisPool(config, ip, port, 5000, null.asInstanceOf[String], db, null.asInstanceOf[String])
    }
    pool.getResource
  }

  def getConnection(ip: String, password: String, port: Int, db: Int): Jedis = {
    if(null == pool){
      pool = new JedisPool(config, ip, port, 5000, password, db, null.asInstanceOf[String])
    }
    pool.getResource
  }

  lazy val hook = new Thread {
    override def run() = {
      if (null != pool) {
        pool.destroy()
      }
    }
  }

  sys.addShutdownHook(hook.run())


  def main(args: Array[String]) {
    val conn: Jedis = JedisConnectionPool.getConnection("192.168.129.151", 7000, 8)
    val r = conn.keys("content_mix_new:1001") //redis是kv对内存数据库；k就相当于主键，v是对应的值
    println(r)
  }
}
