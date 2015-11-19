/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.eagle.datastream

import java.util

import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.topology.{BoltDeclarer, TopologyBuilder}
import backtype.storm.tuple.Fields
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

case class StormTopologyCompiler(config: Config, graph: AbstractStreamProducerGraph) extends AbstractTopologyCompiler{
  val LOG = LoggerFactory.getLogger(StormTopologyCompiler.getClass)
  val boltCache = scala.collection.mutable.Map[StreamProducer, StormBoltWrapper]()

  override def buildTopology: AbstractTopologyExecutor ={
    val builder = new TopologyBuilder();
    val iter = graph.iterator()
    val boltDeclarerCache = scala.collection.mutable.Map[String, BoltDeclarer]()
    while(iter.hasNext){
      val from = iter.next()
      val fromName = from.name
      if(graph.isSource(from)){
        val spout = StormSpoutFactory.createSpout(config, from.asInstanceOf[StormSourceProducer])
        builder.setSpout(fromName, spout, from.parallelism)
        LOG.info("Spout name : " + fromName + " with parallelism " + from.parallelism)
      } else {
        LOG.info("Bolt name:" + fromName)
      }

      val edges = graph.outgoingEdgesOf(from)
      edges.foreach(sc => {
        val toName = sc.to.name
        var boltDeclarer : BoltDeclarer = null
        val toBolt = createBoltIfAbsent(toName)
        boltDeclarerCache.get(toName) match{
          case None => {
            var finalParallelism = 1
            graph.getNodeByName(toName) match {
              case Some(p) => finalParallelism = p.parallelism
              case None => finalParallelism = 1
            }
            boltDeclarer = builder.setBolt(toName, toBolt, finalParallelism);
            LOG.info("created bolt " + toName + " with parallelism " + finalParallelism)
            boltDeclarerCache.put(toName, boltDeclarer)
          }
          case Some(bt) => boltDeclarer = bt
        }
        sc.groupByFields match{
          case Nil => boltDeclarer.shuffleGrouping(fromName)
          case p => boltDeclarer.fieldsGrouping(fromName, new Fields(fields(p)))
        }
        LOG.info("bolt connected " + fromName + "->" + toName + " with groupby fields " + sc.groupByFields)
      })
    }
    new StormTopologyExecutorImpl(builder.createTopology, config)
  }

  def fields(fields : Seq[Int]): java.util.List[String] ={
    val ret = new util.ArrayList[String]
    fields.map(n => ret.add(OutputFieldNameConst.FIELD_PREFIX + n))
    ret
  }

  def createBoltIfAbsent(name : String) : BaseRichBolt = {
    val producer = graph.getNodeByName(name)
    producer match{
      case Some(p) => createBoltIfAbsent(graph, p)
      case None => throw new IllegalArgumentException("please check bolt name " + name)
    }
  }

  def createBoltIfAbsent(graph: AbstractStreamProducerGraph, producer : StreamProducer): BaseRichBolt ={
    boltCache.get(producer) match{
      case Some(bolt) => bolt
      case None => {
        StormBoltFactory.getBoltWrapper(graph, producer, config)
      }
    }
  }
}
