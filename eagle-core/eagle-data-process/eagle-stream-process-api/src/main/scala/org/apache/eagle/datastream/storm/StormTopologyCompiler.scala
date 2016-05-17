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
package org.apache.eagle.datastream.storm

import java.util

import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.{BoltDeclarer, TopologyBuilder}
import backtype.storm.tuple.Fields
import com.typesafe.config.Config
import org.apache.eagle.dataproc.impl.storm.partition.CustomPartitionGrouping
import org.apache.eagle.datastream.core._
import org.apache.eagle.datastream.utils.NameConstants
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

case class StormTopologyCompiler(config: Config, graph: StreamProducerGraph) extends AbstractTopologyCompiler{
  val LOG = LoggerFactory.getLogger(StormTopologyCompiler.getClass)
  val boltCache = scala.collection.mutable.Map[StreamProducer[Any], StormBoltWrapper]()

  override def buildTopology: AbstractTopologyExecutor ={
    val builder = new TopologyBuilder()
    val iter = graph.iterator()
    val boltDeclarerCache = scala.collection.mutable.Map[String, BoltDeclarer]()
    val stormTopologyGraph = ListBuffer[String]()
    while(iter.hasNext){
      val from = iter.next()
      val fromName = from.name
      if(graph.isSource(from)){
        val spout = StormSpoutFactory.createSpout(config, from)
        builder.setSpout(fromName, spout, from.parallelism)
        LOG.info("Spout: " + fromName + " with parallelism " + from.parallelism)
      } else {
        LOG.debug("Bolt" + fromName)
      }
      val edges = graph.outgoingEdgesOf(from)
      edges.foreach(sc => {
        val toName = sc.to.name
        var boltDeclarer: BoltDeclarer = null
        val toBolt = createBoltIfAbsent(toName)
        boltDeclarerCache.get(toName) match {
          case None => {
            var finalParallelism = 1
            graph.getNodeByName(toName) match {
              case Some(p) => finalParallelism = p.parallelism
              case None => finalParallelism = 1
            }
            boltDeclarer = builder.setBolt(toName, toBolt, finalParallelism)
            LOG.info("Bolt: " + toName + " with parallelism " + finalParallelism)
            boltDeclarerCache.put(toName, boltDeclarer)
          }
          case Some(bt) => boltDeclarer = bt
        }

        sc match {
          case GroupbyFieldsConnector(_, _, groupByFields) =>
            boltDeclarer.fieldsGrouping(fromName, new Fields(fields(groupByFields)))
          case GroupbyStrategyConnector(_, _, strategy) =>
            boltDeclarer.customGrouping(fromName, new CustomPartitionGrouping(strategy));
          case GroupbyKeyConnector(_, _, keySelector) =>
            boltDeclarer.fieldsGrouping(fromName, new Fields(NameConstants.FIELD_KEY));
          case ShuffleConnector(_, _) => {
            boltDeclarer.shuffleGrouping(fromName)
          }
          case _ => throw new UnsupportedOperationException(s"Supported stream connector $sc")
        }

        if (graph.isSource(from)) {
          stormTopologyGraph += s"Spout{$fromName}{${from.parallelism}) ~> Bolt{$toName}{${from.parallelism}} in $sc"
        } else {
          stormTopologyGraph += s"Bolt{$fromName}{${from.parallelism}) ~> Bolt{$toName}{${from.parallelism}} in $sc"
        }
      })
    }
    LOG.info(s"Storm topology DAG\n{\n \t${stormTopologyGraph.mkString("\n\t")} \n}")
    new StormTopologyExecutorImpl(builder.createTopology, config)
  }

  def fields(fields : Seq[Int]): java.util.List[String] ={
    val ret = new util.ArrayList[String]
    fields.map(n => ret.add(NameConstants.FIELD_PREFIX + n))
    ret
  }

  def createBoltIfAbsent(name : String) : BaseRichBolt = {
    val producer = graph.getNodeByName(name)
    producer match{
      case Some(p) => createBoltIfAbsent(graph, p)
      case None => throw new IllegalArgumentException("please check bolt name " + name)
    }
  }

  def createBoltIfAbsent(graph: StreamProducerGraph, producer : StreamProducer[Any]): BaseRichBolt ={
    boltCache.get(producer) match{
      case Some(bolt) => bolt
      case None => {
        StormBoltFactory.getBoltWrapper(graph, producer, config)
      }
    }
  }
}
