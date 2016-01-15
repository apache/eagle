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
package org.apache.eagle.stream.dsl.pipeline

import org.apache.eagle.datastream.core.StreamProducer
import org.apache.eagle.stream.dsl.definition.{StreamContext, DataStream}

class PipelineFactory {
  def registerStreamPublisher(module:Module,context:StreamContext):Unit = {
    val stream = new DataStream()
    stream.setName(module.getStream)

    // TODO: Module -> StreamProducer extension management
    val producer = module.getType match {
      case "kafka" => KafkaSourceStreamProducer(module.getData).nameAs(module.getName)
      case _ => throw new IllegalArgumentException(s"Unknown  publisher type ${module.getType}")
    }
    context.getEnvironment.register(producer)
    stream.setProducer(producer)
    context.getStreamManager.setStream(stream)
  }

  def registerStreamSubscriber(module:Module,context:StreamContext):Unit = {
    val stream = context.getStreamManager.getStreamOrException(module.getStream)
    // TODO: Module -> StreamProducer extension management
    val producer = module.getType match {
      case "kafka" => KafkaSinkStreamProducer(module.getData).nameAs(module.getName)
      case "stdout" => KafkaSinkStreamProducer(module.getData).nameAs(module.getName)
      case _ => throw new IllegalArgumentException(s"Unknown subscriber type ${module.getType}")
    }

    context.getEnvironment.register(producer)
    if(stream.getProducer == null){
      stream.setProducer(producer)
    }else{
      stream.sink(producer)
    }
    context.getStreamManager.setStream(stream)
  }
}

object PipelineFactory extends PipelineFactory

// subscriber::kafka =>
case class KafkaSourceStreamProducer[T](context:Map[String,AnyRef]) extends StreamProducer[T]{

}

case class KafkaSinkStreamProducer[T](context:Map[String,AnyRef]) extends StreamProducer[T]