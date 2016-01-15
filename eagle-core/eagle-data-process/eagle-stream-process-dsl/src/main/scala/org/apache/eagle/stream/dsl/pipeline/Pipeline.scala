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

import com.typesafe.config.{ConfigFactory, Config}

import scala.collection.JavaConversions.mapAsScalaMap

class Pipeline(
    var extend:Template = null,
    var dataflow:String = null,
    var config:Config = null,
    var publishers:Seq[Module] = Seq(),
    var subscribers:Seq[Module] = Seq()
)

class Module(name:String,moduleType:String,stream:String,data:Map[String,AnyRef]){
  def getName:String = name
  def getType:String = moduleType
  def getStream:String = stream
  def getData:Map[String,AnyRef] = data
}

object Module{
  /**
   *
   * @param moduleType
   * @param data
   * @return
   */
  def apply(moduleType:String,data:Map[String,AnyRef]):Module = {
      val _data = data.head._2.asInstanceOf[java.util.HashMap[String,AnyRef]]
      new Module(data.head._1, moduleType,_data.get("stream").asInstanceOf[String],_data.toMap)
  }
}

object Pipeline{
  val DATAFLOW = "dataflow"
  val PUBLISHER = "publisher"
  val SUBSCRIBER = "subscriber"
  val EXTEND = "extend"
  val CONFIG = "config"

  def apply(config:Config,namespace:String="pipeline"):Pipeline = {
    val _config = if(namespace == null) config else config.getConfig(namespace)
    val _pipeline = new Pipeline()
    _config.root().unwrapped().foreach(entry =>{
      entry._1 match {
        case DATAFLOW => _pipeline.dataflow = entry._2.asInstanceOf[String]
        case PUBLISHER => _pipeline.publishers = entry._2.asInstanceOf[java.util.HashMap[String,AnyRef]].map(kv =>{Module(kv._1,kv._2.asInstanceOf[java.util.HashMap[String,AnyRef]].toMap)}).toSeq
        case SUBSCRIBER => _pipeline.subscribers = entry._2.asInstanceOf[java.util.HashMap[String,AnyRef]].map(kv =>{Module(kv._1,kv._2.asInstanceOf[java.util.HashMap[String,AnyRef]].toMap)}).toSeq
        case EXTEND => _pipeline.extend = Template.newInstance(entry._2.asInstanceOf[String])
        case CONFIG => _pipeline.config = _config.getConfig(entry._1)
        // case _ => throw new IllegalArgumentException(s"unknown config entry: $entry")
      }
    })
    if(_pipeline.config == null) _pipeline.config = ConfigFactory.empty()
    _pipeline
  }
}