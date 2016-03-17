/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.eagle.datastream.sparkstreaming

import org.apache.eagle.datastream.FlatMapperWrapperForSpark
import org.apache.eagle.datastream.core._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
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
      case  FlatMapProducer(flatMapper) => {
        val func = flatMapper.asInstanceOf[FlatMapperWrapperForSpark[Any,Any]].func
        from.flatMap(func)
      }
      case filter: FilterProducer[Any] => {
        from.filter(filter.fn)
      }
      case mapper: MapperProducer[Any, Any] => {
        from.map(mapper.fn)
      }
      case foreach: ForeachProducer[Any] => {
        from.foreachRDD(rdd => {
          rdd.collect().foreach(foreach.fn)
        })
      }
      case reduceByKeyer: ReduceByKeyProducer[Any,Any] => {
        if(from.isInstanceOf[DStream[(Any,Any)]]){
          from.asInstanceOf[DStream[(Any,Any)]].reduceByKey(reduceByKeyer.fn)
        }
        else{
          throw new UnsupportedOperationException(s"Unsupported DStream: ${from.toString}")
        }
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





