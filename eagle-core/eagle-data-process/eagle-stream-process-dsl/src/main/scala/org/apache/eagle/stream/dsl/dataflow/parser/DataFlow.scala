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
package org.apache.eagle.stream.dsl.dataflow.parser

import com.typesafe.config.Config
import org.apache.eagle.stream.dsl.dataflow.utils.ParseException

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import scala.collection.mutable


class DataFlow {
  def getInputs(id: String):Seq[Processor] = {
    this.getConnectors.filter(_.to.equals(id)).map(c => getProcessor(c.from).get)
  }

  /**
    * Connect if not, do nothing if already connected
    *
    * @param from
    * @param to
    */
  def connect(from: String, to: String): Unit = {
    val connector = Connector(from,to,null)
    var exists = false
    connectors.foreach(c => exists = (c.from.equals(from) && c.to.equals(to)) || exists)
    if(!exists) addConnector(connector)
  }

  private var processors = mutable.Map[String,Processor]()
  private var connectors = mutable.Seq[Connector]()
  def setProcessors(processors:Seq[Processor]):Unit = {
    processors.foreach{module =>
      this.processors.put(module.getId,module)
    }
  }
  def setProcessors(processors:mutable.Map[String,Processor]):Unit = {
    this.processors = processors
  }
  def setConnectors(connectors:Seq[Connector]):Unit = {
    connectors.foreach(connector =>{
      this.connectors :+= connector
    })
  }
  def addProcessor(module:Processor):Unit = {
    if(contains(module)) throw new IllegalArgumentException(s"Duplicated processor id error, ${module.getId} has already been defined as ${getProcessor(module.getId)}")
    processors.put(module.getId,module)
  }

  def contains(module:Processor):Boolean = processors.contains(module.getId)
  def addConnector(connector:Connector):Unit = {
    connectors :+= connector
  }
  def getProcessors:Seq[Processor] = processors.values.toSeq
  def getProcessor(processorId:String):Option[Processor] = processors.get(processorId)
  def getConnectors:Seq[Connector] = connectors
}

case class Processor(var processorId:String = null,var processorType:String = null,var schema:Schema = null, var processorConfig:Map[String,AnyRef] = null) extends Serializable {
  private[dataflow] var inputs:Seq[Processor] = null
  private[dataflow] var inputpIds:Seq[String] = null

  def getId:String = processorId
  def getType:String = processorType
  def getConfig:Map[String,AnyRef] = processorConfig
  def getSchema:Option[Schema] = if(schema == null) None else Some(schema)
}

case class Connector (from:String,to:String, config:Map[String,AnyRef]) extends Serializable{
  import Connector._

  def group:Option[String] = config.get(GROUP_FIELD).asInstanceOf[Option[String]]
  def groupByFields:Option[Seq[String]] = config.get(GROUP_BY_FIELD_FIELD) match {
    case Some(obj) => Some(obj.asInstanceOf[java.util.List[String]].asScala.toSeq)
    case None => None
  }
  def groupByIndexes:Option[Seq[Int]] = config.get(GROUP_BY_INDEX_FIELD) match {
    case Some(obj) => Some(obj.asInstanceOf[java.util.List[java.lang.Integer]].asScala.toSeq.map(Int.unbox(_)))
    case None => None
  }
}

object Connector{
  val GROUP_FIELD = "grouping"
  val GROUP_BY_FIELD_FIELD = "groupByField"
  val GROUP_BY_INDEX_FIELD = "groupByIndex"
}

private [dataflow]
object Processor {
  val SCHEMA_FIELD:String = "schema"
  val INPUTS_FIELD = "inputs"
  def parse(processorId:String,processorType:String,context:Map[String,AnyRef], schemaSet:SchemaSet):Processor = {
    val schema = context.get(SCHEMA_FIELD) match {
      case Some(schemaDef) => schemaDef match {
        case schemaId:String => schemaSet.get(schemaId).getOrElse {
          throw new ParseException(s"Schema [$schemaId] is not found but referred by [$processorType:$processorId] in $context")
        }
        case schemaMap:java.util.HashMap[String,AnyRef] => Schema.parse(schemaMap.toMap)
        case _ => throw new ParseException(s"Illegal value for schema: $schemaDef")
      }
      case None => null
    }
    val instance = new Processor(processorId,processorType,schema,context-SCHEMA_FIELD)
    if(context.contains(INPUTS_FIELD)) instance.inputpIds = context.get(INPUTS_FIELD).get.asInstanceOf[java.util.List[String]].asScala.toSeq
    instance
  }
}


trait DataFlowParser {
  def parse(config:Config,schemaSet:SchemaSet = SchemaSet.empty()):DataFlow = {
    val dataw = new DataFlow()
    val map = config.root().unwrapped().toMap

    // Parse processors and connectors
    map.foreach(entry => {
      parseSingle(entry._1,entry._2.asInstanceOf[java.util.HashMap[String,AnyRef]].toMap,dataw,schemaSet)
    })
    expand(dataw)
    validate(dataw)
    dataw
  }

  private def expand(datafw: DataFlow):Unit = {
    datafw.getProcessors.foreach(proc =>{
      if(proc.inputpIds!=null) {
        proc.inputpIds.foreach(id => {
          // connect if not
          datafw.connect(id,proc.getId)
        })
      }
      proc.inputs = datafw.getInputs(proc.getId)
    })
  }

  private def
  validate(pipeline:DataFlow): Unit ={
    def checkModuleExists(id:String): Unit ={
      pipeline.getProcessor(id).orElse {
        throw new ParseException(s"Stream [$id] is not defined before being referred")
      }
    }

    pipeline.getConnectors.foreach {connector =>
      checkModuleExists(connector.from)
      checkModuleExists(connector.to)
    }
  }

  private def
  parseSingle(identifier:String,config:Map[String,AnyRef],dataflow:DataFlow, schemaSet: SchemaSet):Unit = {
    Identifier.parse(identifier) match {
      case DefinitionIdentifier(processorType) => {
        config foreach {entry =>
          dataflow.addProcessor(Processor.parse(entry._1, processorType,entry._2.asInstanceOf[java.util.HashMap[String, AnyRef]].toMap,schemaSet))
        }
      }
      case ConnectionIdentifier(fromIds,toId) => fromIds.foreach { fromId =>
        if(fromId.eq(toId)) throw new ParseException(s"Can't connect $fromId to $toId")
        dataflow.addConnector(Connector(fromId,toId,config))
      }
      case _ => ???
    }
  }
}


private[dataflow] trait Identifier

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
      case None => {
        if(identifier.contains(ConnectorFlag)) throw new ParseException(s"Failed to parse $identifier")
        DefinitionIdentifier(identifier)
      }
    }
  }
}

object DataFlow extends DataFlowParser