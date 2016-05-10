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

import com.typesafe.config.Config
import org.apache.eagle.datastream.core.{AbstractTopologyExecutor, AbstractTopologyCompiler, StreamProducerGraph}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


case class SparkStreamingCompiler (config: Config, graph: StreamProducerGraph) extends AbstractTopologyCompiler{
  override def buildTopology: AbstractTopologyExecutor ={
    val LOG = LoggerFactory.getLogger(SparkStreamingCompiler.getClass)
    val appName = config.getString("envContextConfig.topologyName")
    val conf = new SparkConf().setAppName(appName)
    val localMode: Boolean = config.getString("envContextConfig.mode").equalsIgnoreCase("local")
    if (!localMode) {
      LOG.info("Running in cluster mode")

    } else {
      LOG.info("Running in local mode")
      //System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\hadoop-common-2.2.0-bin-master");
      conf.setMaster("local[8]")
    }

    val ssc = new StreamingContext(conf, Seconds(1))
    val iter = graph.iterator()
    while(iter.hasNext){
      val from = iter.next()
      if(graph.isSource(from)){
        val dStream = DStreamFactory.createInputDStream(ssc,from)
        val edges = graph.outgoingEdgesOf(from)
        edges.foreach(sc => {
          val producer = graph.getNodeByName(sc.to.name)
          producer match {
            case Some(p) => {
              if (dStream.isInstanceOf[DStream[Any]]) {
                //dStream.map(o => o)
                DStreamFactory.createDStreamsByDFS(graph, dStream.asInstanceOf[DStream[Any]], p)
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
    new SparkStreamingExecutorImpl(ssc, config)
  }
}
