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
package org.apache.eagle.stream.dsl.definition

import org.slf4j.LoggerFactory

import scala.collection.mutable

class UnknownStreamException(message:String = null,throwable:Throwable = null) extends RuntimeException(message,throwable)

class DataStreamManager {
  private val streamMap = mutable.Map[String,DataStream]()
  private val logger = LoggerFactory.getLogger(classOf[DataStreamManager])

  def getStream(name:String):DataStream = {
    if(streamMap.contains(name)) {
      streamMap(name)
    } else throw new UnknownStreamException(s"Stream '$name' is not defined")
  }

  def setStream(stream:DataStream) = {
    val name = stream.getName
    if(name == null){
      throw new NullPointerException(s"Name of $stream is null")
    }else if(streamMap.contains(stream.getName)) {
      logger.info(s"Redefine stream $name (${streamMap(name)}) as $stream")
    }else{
      logger.info(s"Define new stream '$name' as $stream")
    }
    streamMap(name) = stream
  }
  def clear():Unit = {
    streamMap.clear()
  }
}

object DataStreamManager extends DataStreamManager