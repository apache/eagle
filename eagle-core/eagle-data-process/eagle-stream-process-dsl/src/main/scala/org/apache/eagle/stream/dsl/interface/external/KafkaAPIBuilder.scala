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
package org.apache.eagle.stream.dsl.interface.external

import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider
import org.apache.eagle.datastream.core.StreamProducer
import org.apache.eagle.datastream.storm.StormExecutionEnvironment
import org.apache.eagle.stream.dsl.interface.BaseAPIBuilder

trait KafkaAPIBuilder extends BaseAPIBuilder{
  /**
   * kafka interface without parameters
   * @return
   */
  def kafka:StreamProducer[AnyRef] = this.context.getEnvironment match {
    case e:StormExecutionEnvironment =>
      e.fromSpout(new KafkaSourcedSpoutProvider())
    case e@_ => throw new IllegalStateException(s"kafka only supports as source (i.e. spout) for storm now, but not support environment $e")
  }
}