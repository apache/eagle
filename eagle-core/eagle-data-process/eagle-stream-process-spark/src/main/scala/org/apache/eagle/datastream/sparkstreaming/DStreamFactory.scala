/*
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

import java.util
import java.util.Properties

import org.apache.eagle.alert.executor.AlertExecutor
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutScheme
import org.apache.eagle.datastream.Collector
import org.apache.eagle.datastream.core._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable

object DStreamFactory {

  def createInputDStream(ssc: StreamingContext, from: StreamProducer[Any]): DStream[Any] = {
    implicit val streamInfo = from.getInfo

    from match {
      case p@IterableStreamProducer(iterable, recycle) => {
        ssc.receiverStream(new IterableReceiver(iterable, recycle))
      }
      case p@SparkStreamingKafkaSourceProducer() =>{
        val topic = p.config.getString("dataSourceConfig.topic");
        val zkConnection = p.config.getString("dataSourceConfig.zkConnection");
        val groupId = p.config.getString("dataSourceConfig.consumerGroupId");

        val deserClsName = p.config.getString("dataSourceConfig.deserializerClass")
        val deserializer = new KafkaSourcedSpoutScheme(deserClsName,p.config)

        val prop: Properties = new Properties
        if (p.config.hasPath("eagleProps")) {
          prop.putAll(p.config.getObject("eagleProps"))
         }

         //val deserializer = new JsonMessageDeserializer(prop);
        val D = KafkaUtils.createStream(ssc,zkConnection,groupId,Map(topic -> 1))
        D.asInstanceOf[DStream[Any]].map(o => o match {
          case Tuple2(a,b) => {
            if(b.isInstanceOf[String]) {
              val result = deserializer.deserialize(b.asInstanceOf[String].getBytes())
              Tuple2(a, result.get(0))
            }
            else Tuple2(a,b)
          }
          case a => a
         })
    }
      case _ =>
        throw new IllegalArgumentException(s"Cannot compile unknown $from to a Storm Spout")
    }
  }

  class TraversableCollector(buffer: mutable.ListBuffer[Any]) extends Collector[Any]{
    override def collect(r: Any): Unit = {
      buffer += r
    }
  }

  def tupleToSeq(tuple:Any):Seq[AnyRef]={
    tuple match {
      case scala.Tuple1(a) => Seq(util.Arrays.asList(a.asInstanceOf[AnyRef]))
      case scala.Tuple2(a, b) => Seq(util.Arrays.asList(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef]))
      case scala.Tuple3(a, b, c) => {

        if(a == null) Seq("test", b.asInstanceOf[AnyRef], c.asInstanceOf[AnyRef])
        else Seq(util.Arrays.asList(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef], c.asInstanceOf[AnyRef]))
      }
      case scala.Tuple4(a, b, c, d) => Seq(util.Arrays.asList(a.asInstanceOf[AnyRef], b.asInstanceOf[AnyRef], c.asInstanceOf[AnyRef], d.asInstanceOf[AnyRef]))
      case a => Seq(util.Arrays.asList(a.asInstanceOf[AnyRef]))
    }
  }

  def createDStreamsByDFS(graph: StreamProducerGraph, from: DStream[Any], to: StreamProducer[Any]): Unit = {
    implicit val streamInfo = to.getInfo
    val dStream = to match {
      case  FlatMapProducer(flatMapper) => {
        if (flatMapper.isInstanceOf[AlertExecutor]) {
          flatMapper.asInstanceOf[AlertExecutor].prepareConfig(to.config)

          from.flatMap(input => {
            val result = mutable.ListBuffer[Any]()
            flatMapper.flatMap(tupleToSeq(input),new TraversableCollector(result))
            result
          })
        }
        else {
          from.flatMap(input => {
            val result = mutable.ListBuffer[Any]()
            flatMapper.flatMap(Seq(input).asInstanceOf[Seq[AnyRef]],new TraversableCollector(result))
            result
          })
        }
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
    //if no output, force to output!
    if(edges.size == 0 && dStream.isInstanceOf[DStream[Any]]){
      dStream.asInstanceOf[DStream[Any]].foreachRDD(rdd => {
        rdd.collect().foreach(println)
      })
    }
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





