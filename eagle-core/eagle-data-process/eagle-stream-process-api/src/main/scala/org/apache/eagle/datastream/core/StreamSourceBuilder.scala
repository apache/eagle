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
  def from[T](iterable: Iterable[T],recycle:Boolean = false)(implicit typeTag:ru.TypeTag[T]):IterableStreamProducer[T]={
    val p = IterableStreamProducer[T](iterable,recycle)
    p.setup(dag,config.get)(typeTag)
    p
  }
}