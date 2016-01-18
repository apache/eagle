package org.apache.eagle.stream.dsl.definition

import org.apache.eagle.datastream.core.StreamProducer

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
trait StreamDefinition {
  private var name:String = null
  private var schema:StreamSchema = null
  private var streamProducer:StreamProducer[Any] = null
  private var startStreamProducer:StreamProducer[Any] = null

  def setSchema(schema: StreamSchema): Unit = this.schema = schema
  def getSchema: Option[StreamSchema] = if(this.schema == null) None else Some(schema)
  def getSchemaOrException: StreamSchema = if(this.schema == null) throw new StreamUndefinedException(s"Schema of stream $this is not defined") else schema

  def setProducer(producer:StreamProducer[Any]) = {
    this.streamProducer = producer
    if(this.getStartProducer == null){
      this.setStartProducer(producer)
    }
  }

  def getProducer = this.streamProducer

  def getStartProducer = startStreamProducer
  def setStartProducer(producer:StreamProducer[Any]) = startStreamProducer = producer

  def setName(name:String):Unit = {
    this.name = name
  }
  def getName:String = this.name
}
