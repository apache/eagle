package org.apache.eagle.datastream.sparkstreaming

import org.apache.eagle.datastream.core._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by zqin on 2016/1/25.
  */
object DStreamFactory {

  def createInputDStream(ssc: StreamingContext, from: StreamProducer[Any]): DStream[Any] = {
    implicit val streamInfo = from.getInfo

    from match {
      case p@IterableStreamProducer(iterable, recycle) => {
        ssc.receiverStream(new IterableReceiver(iterable, recycle))
      }
      case _ =>
        throw new IllegalArgumentException(s"Cannot compile unknown $from to a Storm Spout")
    }
  }

  def createDStreamsByDFS(graph: StreamProducerGraph, from: DStream[Any], to: StreamProducer[Any]): Unit = {
    implicit val streamInfo = to.getInfo
    val dStream = to match {

      case FlatMapProducer(worker) => {

      }
      case filter: FilterProducer[Any] => {
        from.filter(filter.fn)
      }
      case mapper: MapperProducer[Any, Any] => {
        from.map(mapper.fn)
      }
      case foreach: ForeachProducer[Any] => {
        from.foreachRDD(foreach.fn)
      }
      case _ => throw new UnsupportedOperationException(s"Unsupported producer: ${to.toString}")
    }

    val edges = graph.outgoingEdgesOf(to)
    edges.foreach(sc => {
      val producer = graph.getNodeByName(sc.to.name)
      producer match {
        case Some(p) => {
          if (dStream.isInstanceOf[DStream[Any]]) {
            createDStreamsByDFS(graph, dStream.asInstanceOf[DStream[Any]], p)
          }
          else {
            throw throw new IllegalArgumentException("Can not create DStream from " + dStream.toString)
          }
        }
        case None => throw new IllegalArgumentException("please check bolt name " + sc.to.name)
      }
    })
  }
}





