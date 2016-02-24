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
        if(worker.isInstanceOf[JavaStormStreamExecutor[AnyRef]]){
          worker.asInstanceOf[JavaStormStreamExecutor[AnyRef]].prepareConfig(config)
          JavaStormBoltWrapper(worker.asInstanceOf[JavaStormStreamExecutor[AnyRef]])
        }else if(worker.isInstanceOf[StormStreamExecutor[AnyRef]]){
          worker.asInstanceOf[StormStreamExecutor[AnyRef]].prepareConfig(config)
          StormBoltWrapper(worker.asInstanceOf[StormStreamExecutor[AnyRef]])
        }else if(worker.isInstanceOf[FlatMapperWrapper[Any]]){
          StormFlatFunctionWrapper(worker.asInstanceOf[FlatMapperWrapper[Any]].func)
        } else {
          StormFlatMapperWrapper(worker)
        }
//        else {
//          throw new UnsupportedOperationException(s"Unsupported FlatMapperProducer type: $producer")
//        }
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
        val persisExecutor = new PersistExecutor(persist.executorId, persist.storageType.toString)
        persisExecutor.prepareConfig(config)
        JavaStormBoltWrapper(persisExecutor.asInstanceOf[JavaStormStreamExecutor[AnyRef]])
      }
      case _ => throw new UnsupportedOperationException(s"Unsupported producer: ${producer.toString}")
    }
  }
}