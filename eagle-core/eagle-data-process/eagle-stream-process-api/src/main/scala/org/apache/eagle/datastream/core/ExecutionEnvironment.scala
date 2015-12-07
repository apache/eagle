package org.apache.eagle.datastream.core

import com.typesafe.config.Config
import org.apache.eagle.datastream.utils.GraphPrinter
import org.jgrapht.experimental.dag.DirectedAcyclicGraph

/**
 * @since 0.3.0
 */
trait ExecutionEnvironment {
  def config:Configuration

  /**
   * Business logic DAG
   * @return
   */
  def dag:DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]

  /**
   * Start to execute
   */
  def execute():Unit

  /**
   * Support Java Style Config
   *
   * @return
   */
  def getConfig:Config = config.get
}

/**
 * @todo Use Configuration instead of Config
 *
 * @param conf
 */
abstract class ExecutionEnvironmentBase(private val conf:Config)  extends ExecutionEnvironment with StreamSourceBuilder {
  private val _dag = new DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]](classOf[StreamConnector[Any,Any]])
  implicit private val _config:Configuration = Configuration(conf)

  override def dag = _dag
  override def config = _config

  override def execute(): Unit = {
    // Must set default name before graph printing
    StreamNameExpansion(config.get).expand(dag)
    GraphPrinter.print(dag,"Before expanded DAG ")
    StreamAlertExpansion(config.get).expand(dag)
    StreamUnionExpansion(config.get).expand(dag)
    StreamGroupbyExpansion(config.get).expand(dag)
    StreamParallelismConfigExpansion(config.get).expand(dag)
    GraphPrinter.print(dag,"After expanded DAG ")
    val streamDAG = StreamDAGTransformer.transform(dag)
    execute(streamDAG)
  }

  protected def execute(dag: StreamDAG)
}