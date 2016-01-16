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
package org.apache.eagle.stream.dsl.dataflow.compiler

import org.apache.eagle.datastream.core._
import org.apache.eagle.stream.dsl.dataflow.parser._

trait PipelineCompiler {
  val classOfProcessorType = Map[String,StreamFactory](
    "KafkaSource" -> KafkaSourceStreamProducer,
    "KafkaSink" -> KafkaSinkStreamProducer,
    "Alert" -> AlertStreamProducer,
    "Persistence" -> PersistProducer
  )

  def compile(pipeline:Pipeline):StreamContext = {
    val context = new StreamContext(pipeline.config)
    val dataflow = pipeline.dataflow
    val dag = new StreamDAG(context.dag)
    dataflow.getProcessors.map(buildStreamProducer(dag,_)).foreach(producer =>{
      producer.initWith(dag.graph,pipeline.config)
      dag.addVertex(producer)
    })
    dataflow.getConnectors.foreach(connector =>{
      val from = dag.getNodeByName(connector.from).get
      val to = dag.getNodeByName(connector.to).get
      dag.addEdge(from,to,buildStreamConnector(from,to,dataflow,connector))
    })
    context
  }

  private def  buildStreamProducer(dag:StreamDAG,processor:Processor):StreamProducer[Any] = {
    if(classOfProcessorType.contains(processor.getType)){
      classOfProcessorType(processor.getType).createInstance(processor.getConfig).nameAs(processor.getId)
    } else {
      throw new CompileException(s"Unknown processor type [${processor.getType}]")
    }
  }

  private def buildStreamConnector(from:StreamProducer[Any],to:StreamProducer[Any],dataflow:DataFlow,connector:Connector):StreamConnector[Any,Any]={
    var groupByIndexes:Seq[Int] = connector.groupByIndexes.orNull
    if(groupByIndexes!=null ){
      if(connector.groupByFields.isDefined) throw new CompileException(s"Both ${Connector.GROUP_BY_FIELD_FIELD} and ${Connector.GROUP_BY_INDEX_FIELD} is defined at same time")
    } else if(connector.groupByFields.isDefined){
      groupByIndexes = connector.groupByFields.get.map(dataflow.getProcessor(from.name).get.getSchema.get.indexOfAttribute)
    }
    if(groupByIndexes == null){
      GroupbyFieldsConnector(from,to,groupByIndexes)
    } else {
      ShuffleConnector(from,to)
    }
  }
}

trait StreamFactory{
  def getType:String
  def createInstance(config:Map[String,AnyRef]):StreamProducer[Any]
}

object KafkaSourceStreamProducer extends StreamFactory{
  def getType = "KafkaSource"
  override def createInstance(config:Map[String,AnyRef]): StreamProducer[Any] = new KafkaSourceStreamProducer(config)
}

object KafkaSinkStreamProducer extends StreamFactory{
  def getType = "KafkaSink"
  override def createInstance(config:Map[String,AnyRef]): StreamProducer[Any] = new KafkaSinkStreamProducer(config)
}

object AlertStreamProducer extends StreamFactory{
  def getType:String = "alert"
  override def createInstance(config:Map[String,AnyRef]): StreamProducer[Any] = {
    // Support create functional AlertStreamProducer constructor
    new AlertStreamProducer(config.getOrElse("upStreamNames",null).asInstanceOf[java.util.List[String]],config.getOrElse("alertExecutorId","defaultAlertExecutor").asInstanceOf[String],config.getOrElse("consume",true).asInstanceOf[Boolean])
  }
}

object PersistProducer extends StreamFactory{
  override def getType = "Persistence"
  override def createInstance(config: Map[String, AnyRef]): StreamProducer[Any] = {
    new PersistProducer(config.getOrElse("executorId","defaultExecutorId").asInstanceOf[String],StorageType.withName(config.getOrElse("storageType",null).asInstanceOf[String]))
  }
}

case class KafkaSourceStreamProducer[T](context:Map[String,AnyRef]) extends StreamProducer[T]
case class KafkaSinkStreamProducer[T](context:Map[String,AnyRef]) extends StreamProducer[T]