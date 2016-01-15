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
package org.apache.eagle.stream.dsl.dataflow

import com.typesafe.config.Config

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable

private [dataflow]
class DataFlow {
  private var modules = mutable.Map[String,Module]()
  private var connectors = mutable.Seq[Connector]()
  def setModules(modules:Seq[Module]):Unit = {
    modules.foreach{module =>
      this.modules.put(module.getModuleId,module)
    }
  }
  def setModules(modules:mutable.Map[String,Module]):Unit = {
    this.modules = modules
  }
  def setConnectors(connectors:Seq[Connector]):Unit = {
    connectors.foreach(connector =>{
      this.connectors :+= connector
    })
  }

  def addModule(module:Module):Unit = {
    if(contains(module)) throw new IllegalArgumentException(s"Duplicated module id error, ${module.getModuleId} has already been taken by ${getModuleById(module.getModuleId)}")
    modules.put(module.getModuleId,module)
  }

  def contains(module:Module):Boolean = modules.contains(module.getModuleId)

  def addConnector(connector:Connector):Unit = {
    connectors :+= connector
  }

  def getModules:Seq[Module] = modules.values.toSeq
  def getModuleById(moduleId:String):Option[Module] = modules.get(moduleId)
  def getConnectors:Seq[Connector] = connectors
}

object DataFlow {
  def parse(config:Config):DataFlow = DataFlowParser.parse(config)
}

private [dataflow]
case class Module(var moduleId:String,var moduleType:String,var context:Map[String,AnyRef]) extends Serializable {
  def getModuleId:String = moduleId
  def getModuleType:String = moduleType
  def getContext:Map[String,AnyRef] = context
}

private [dataflow]
object Module {
  def apply(moduleType:String,context:Map[String,AnyRef]) = {
    val data = context.head._2.asInstanceOf[java.util.HashMap[String,AnyRef]].toMap
    new Module(data.head._1, moduleType,data)
  }
}

private [dataflow]
case class Connector (from:String,to:String, config:Map[String,AnyRef]) extends Serializable