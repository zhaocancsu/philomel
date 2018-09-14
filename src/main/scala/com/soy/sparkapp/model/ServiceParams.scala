package com.soy.sparkapp.model

import com.soy.sparkapp.model.FieldDataTypes.FieldDataTypes
import com.soy.sparkapp.sparkfunc.RedisStorageType.RedisStorageType

import scala.collection.mutable

/**
  * author  soy
  * version  [版本号, 2017/2/18]
  * see  [相关类/方法]
  * since  [产品/模块版本]
  */
class ServiceParams extends Serializable{

  //spark资源核数
  var coresNum: String = _

  //executor内存大小
  var memorySize: String = _

  //spark master地址
  var sparkMasterUrl: String = _

  //hdfs路径前缀
  var hdfsPathPrefix: String = _

  //执行sql
  var sql: String = _

  //执行sql列表
  var sqlList: List[String] = _

  //目标路径
  var targetPath: String = _

  //json对象字符串
  var objectJson: String = _

  //表名
  var tableName: String = _

  //文件路径
  var filePath: String = _

  //文件分隔符
  var splitChar: Char = ','

  //分隔符
  var delimiter: String = "^"

  //地址(redis)
  var host: String = _

  //密码(redis)
  var password: String = _

  //端口(redis)
  var port: Int = _

  //database index(redis)
  var dbIndex: Int = _

  //redis存储格式
  var redisStorgeType: RedisStorageType = _

  //表项schema
  var fields: mutable.Map[String, FieldDataTypes] = _

  //表项schema
  var fieldsList: List[FieldStruct] = _

  //业务Id(根据参数jobCode和TaskCode组合)
  var serviceId: String = _

  //数据库连接配置信息
  var dbConfigParams: DbConfigParams = _

  //jdbc连接分区信息
  var jdbcPartitionParams: JdbcPartitionParams = _

  //输出redis是否使用pipeline
  var redisPipeLine: Boolean = false

}
