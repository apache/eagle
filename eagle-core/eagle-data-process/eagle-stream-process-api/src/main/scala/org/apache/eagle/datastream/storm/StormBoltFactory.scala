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
package org.apache.eagle.datastream.storm

import backtype.storm.topology.base.BaseRichBolt
import com.typesafe.config.Config
import org.apache.eagle.dataproc.impl.persist.PersistExecutor
import org.apache.eagle.datastream._
import org.apache.eagle.datastream.core._

object StormBoltFactory {
  def getBoltWrapper(graph: StreamProducerGraph, producer : StreamProducer[Any], config : Config) : BaseRichBolt = {
    implicit val streamInfo = producer.getInfo
    producer match{
      case FlatMapProducer(worker) => {
        if(worker.isInstanceOf[JavaStormStreamExecutor[EagleTuple]]){
          worker.asInstanceOf[JavaStormStreamExecutor[EagleTuple]].prepareConfig(config)
          JavaStormBoltWrapper(worker.asInstanceOf[JavaStormStreamExecutor[EagleTuple]])
        }else if(worker.isInstanceOf[StormStreamExecutor[EagleTuple]]){
          worker.asInstanceOf[StormStreamExecutor[EagleTuple]].prepareConfig(config)
          StormBoltWrapper(worker.asInstanceOf[StormStreamExecutor[EagleTuple]])
        }else {
          throw new UnsupportedOperationException
        }
      }
      case filter:FilterProducer[Any] => {
        FilterBoltWrapper(filter.fn)
      }
      case mapper:MapperProducer[Any,Any] => {
        MapBoltWrapper(mapper.numOutputFields, mapper.fn)
      }
      case foreach:ForeachProducer[Any] => {
        ForeachBoltWrapper(foreach.fn)
      }
      case persist : PersistProducer[Any] => {
        val persisExecutor = new PersistExecutor(persist.executorId, persist.storageType.toString());
        persisExecutor.prepareConfig(config);
        JavaStormBoltWrapper(persist.asInstanceOf[JavaStormStreamExecutor[EagleTuple]])
      }
      case _ => throw new UnsupportedOperationException(s"Unsupported producer: ${producer.toString}")
    }
  }
}