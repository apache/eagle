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

class ParseException(message:String) extends Exception(message)
class CompileException(message:String) extends Exception(message)

object DataFlowParser {
  def parse(config:Config):DataFlow = {
    val dataflow = new DataFlow()
    val map = config.root().unwrapped().toMap

    // 1. Scan pipeline global configuration
    loadGlobalConfig(map)
    // 2. Do parsing
    map.foreach(entry => {
      parse(entry._1,entry._2.asInstanceOf[java.util.HashMap[String,AnyRef]].toMap,dataflow)
    })
    validate(dataflow)

    dataflow
  }

  private
  def validate(pipeline:DataFlow): Unit ={
    def checkModuleExists(id:String): Unit ={
      pipeline.getModuleById(id).orElse {
        throw new ParseException(s"Stream [$id] is not defined before being referred")
      }
    }

    pipeline.getConnectors.foreach {connector =>
      checkModuleExists(connector.from)
      checkModuleExists(connector.to)
    }
  }

  /**
   * Scan pipeline global configuration
   *
   * @param config
   */
  private
  def loadGlobalConfig(config:Map[String,AnyRef]):Unit = {
    // map.foreach(entry =>{
    //  entry._1 match {
    //  }
    // })
  }

  private def parse(identifier:String,config:Map[String,AnyRef],dataflow:DataFlow):Unit = {
    Identifier.parse(identifier) match {
      case DefinitionIdentifier(moduleType) => {
        config foreach {entry =>
          dataflow.addModule(Module(entry._1, moduleType, entry._2.asInstanceOf[java.util.HashMap[String, AnyRef]].toMap))
        }
      }
      case ConnectionIdentifier(fromIds,toId) => fromIds.foreach { fromId =>
        if(fromId.eq(toId)) throw new IllegalStateException(s"Can't connect $fromId to $toId")
        dataflow.addConnector(Connector(fromId,toId,config))
      }
      case _ => throw new IllegalStateException
    }
  }
}

private trait Identifier

private[dataflow] case class DefinitionIdentifier(moduleType: String) extends Identifier
private[dataflow] case class ConnectionIdentifier(fromIds: Seq[String], toId: String) extends Identifier

private[dataflow] object Identifier {
  val ConnectorFlag = "->"
  val UnitFlagSplitPattern = "\\|"
  val UnitFlagChar = "|"
  val ConnectorPattern = s"([\\w-|\\s]+)\\s+$ConnectorFlag\\s+([\\w-_]+)".r
  def parse(identifier: String): Identifier = {
    // ${id} -> ${id}
    ConnectorPattern.findFirstMatchIn(identifier) match {
      case Some(matcher) => {
        if(matcher.groupCount != 2){
          throw new ParseException(s"Illegal connector definition: $identifier")
        }else{
          val source = matcher.group(1)
          val destination = matcher.group(2)
          if(source.contains(UnitFlagChar)) {
            val sources = source.split(UnitFlagSplitPattern).toSeq
            ConnectionIdentifier(sources.map{_.trim()},destination)
          }else{
            ConnectionIdentifier(Seq(source),destination)
          }
        }
      }
      case None => DefinitionIdentifier(identifier)
    }
  }
}