package org.apache.eagle.datastream.core

import org.jgrapht.experimental.dag.DirectedAcyclicGraph

import scala.reflect.runtime.{universe => ru}

/**
 * @since  12/7/15
 */
trait StreamSourceBuilder {
  def config:Configuration

  /**
   * Business logic DAG
   * @return
   */
  def dag:DirectedAcyclicGraph[StreamProducer[Any], StreamConnector[Any,Any]]

  /**
   *
   * @param iterable top level Iterable interface
   * @param recycle
   * @tparam T
   * @return
   */
  def from[T:ru.TypeTag](iterable: Iterable[T],recycle:Boolean = false):IterableStreamProducer[T]={
    val p = IterableStreamProducer[T](iterable,recycle)
    p.init(dag,config.get)
    p
  }
}