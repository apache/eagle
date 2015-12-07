package org.apache.eagle.datastream.storm

import backtype.storm.topology.base.BaseRichSpout
import com.typesafe.config.Config
import org.apache.eagle.dataproc.impl.storm.StormSpoutProvider
import org.apache.eagle.datastream.core.{ExecutionEnvironmentBase, StormSourceProducer, StreamDAG}

import scala.reflect.runtime.{universe => ru}

/**
 * @since  12/7/15
 */
case class StormExecutionEnvironment(private val conf:Config) extends ExecutionEnvironmentBase(conf){


  override def execute(dag: StreamDAG) : Unit = {
    StormTopologyCompiler(config.get, dag).buildTopology.execute
  }

  def fromSpout[T](source: BaseRichSpout): StormSourceProducer[T] = {
    val ret = StormSourceProducer[T](source)
    ret.init[java.util.List[_]](dag ,config.get)
    ret
  }

  def fromSpout[T](sourceProvider: StormSpoutProvider):StormSourceProducer[T] = fromSpout(sourceProvider.getSpout(config.get))
}