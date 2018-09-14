package com.soy.sparkapp.model

case class DbConfigParams(connectUrl: String,driverClassName: String,userName: String,password: String) extends Serializable
case class JdbcPartitionParams(numPartitions: String,lowerBound: String,upperBound: String) extends Serializable
