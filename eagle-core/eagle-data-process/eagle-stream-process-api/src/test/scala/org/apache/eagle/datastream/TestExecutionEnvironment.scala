package org.apache.eagle.datastream

import com.typesafe.config.ConfigFactory
import org.apache.eagle.datastream.storm.StormExecutionEnvironment

/**
 * @since  12/5/15
 */
object TestExecutionEnvironment extends App{
  val env0 = ExecutionEnvironments.get[StormExecutionEnvironment]
  println(env0)
  val config = ConfigFactory.load()
  val env1 = ExecutionEnvironments.get[StormExecutionEnvironment](config)
  println(env1)
  val env2 = ExecutionEnvironments.get[StormExecutionEnvironment](Array[String]("-D","key=value"))
  println(env2)
}
