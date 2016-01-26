package org.apache.eagle.datastream.sparkstreaming

import com.typesafe.config.Config
import org.apache.eagle.datastream.core.{StreamDAG, ExecutionEnvironment}
/**
  * Created by zqin on 2016/1/21.
  */
class SparkStreamingExecutionEnvironment (private val conf:Config) extends ExecutionEnvironment(conf){
  override def execute(dag: StreamDAG) : Unit = {
    SparkStreamingCompiler(config.get, dag).buildTopology.execute
  }
}
