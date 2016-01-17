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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.config.ConfigFactory
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider
import org.apache.eagle.datastream.core._
import org.apache.eagle.partition.PartitionStrategy
import org.apache.eagle.stream.dsl.dataflow.parser._
import org.apache.eagle.stream.dsl.dataflow.utils.CompileException
import org.apache.kafka.clients.producer.{RecordMetadata, Callback, ProducerRecord, KafkaProducer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

trait PipelineCompiler {
  val classOfProcessorType = Map[String,ModuleGenerator](
    "KafkaSource" -> KafkaSourceStreamProducer,
    "KafkaSink" -> KafkaSinkStreamProducer,
    "Alert" -> AlertStreamProducer,
    "Persistence" -> PersistProducer,
    "Console" -> ConsoleStreamProducer
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
      classOfProcessorType(processor.getType).generate(processor.getId,processor.getConfig).nameAs(processor.getId)
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
      ShuffleConnector(from,to)
    } else {
      GroupbyFieldsConnector(from,to,groupByIndexes)
    }
  }
}

trait ModuleGenerator{
  def getType:String
  def generate(moduleId:String,config:Map[String,AnyRef]):StreamProducer[Any]
}

object KafkaSourceStreamProducer extends ModuleGenerator{
  def getType = "KafkaSource"
  override def generate(moduleId:String,config:Map[String,AnyRef]): StreamProducer[Any] =
    new StormSourceProducer[Any](new KafkaSourcedSpoutProvider(null).getSpout(ConfigFactory.parseMap(config.asJava)))
}

object KafkaSinkStreamProducer extends ModuleGenerator{
  def getType = "KafkaSink"
  override def generate(moduleId:String,config:Map[String,AnyRef]): StreamProducer[Any] = ForeachProducer[AnyRef](KafkaSinkExecutor(config))
}

object ConsoleStreamProducer extends ModuleGenerator{
  override def getType: String = "Stdout"
  override def generate(moduleId:String,config: Map[String, AnyRef]): StreamProducer[Any] = ForeachProducer[Any](m=>print(s"$m\n"))
}

object AlertStreamProducer extends ModuleGenerator{
  def getType:String = "Alert"
  override def generate(moduleId:String,config:Map[String,AnyRef]): StreamProducer[Any] = {
    // Support create functional AlertStreamProducer constructor
    new AlertStreamProducer (
      upStreamNames = config.getOrElse("upStreamNames",null).asInstanceOf[java.util.List[String]],
      alertExecutorId = config.getOrElse("alertExecutorId",moduleId).asInstanceOf[String],
      consume = config.getOrElse("consume",true).asInstanceOf[Boolean],
      strategy = config.get("strategy") match {case Some(strategy)=> Class.forName(strategy.asInstanceOf[String]).newInstance().asInstanceOf[PartitionStrategy] case None => null}
    )
  }
}

object PersistProducer extends ModuleGenerator{
  override def getType = "Persistence"
  override def generate(moduleId:String,config: Map[String, AnyRef]): StreamProducer[Any] = {
    new PersistProducer(config.getOrElse("executorId","defaultExecutorId").asInstanceOf[String],StorageType.withName(config.getOrElse("storageType",null).asInstanceOf[String]))
  }
}

object AggregatorProducer extends ModuleGenerator{
  override def getType: String = "Aggregator"
  override def generate(moduleId:String,config: Map[String, AnyRef]): StreamProducer[Any] = {
    new AggregateProducer(
      config.get("upStreamNames") match {case Some(streams) => streams.asInstanceOf[java.util.List[String]] case None => null},
      config.get("analyzerId") match {case Some(id)=>id.asInstanceOf[String] case None => null},
      config.get("cepQl") match {case Some(sql)=> sql.asInstanceOf[String] case None => null},
      config.get("strategy") match {case Some(strategy)=> Class.forName(strategy.asInstanceOf[String]).newInstance().asInstanceOf[PartitionStrategy] case None => null}
    )
  }
}

/**
  * @todo currently support single topic now, should support topic selector
  * @param config
  */
case class KafkaSinkExecutor(config:Map[String,AnyRef]) extends ((AnyRef) => Unit) with Serializable{
  val TOPIC_KEY = "topic"
  def getDefaultProps = {
    val props = new Properties()
    props.putAll(Map[String,AnyRef](
      "bootstrap.servers" -> "localhost:6667",
      "acks" -> "all",
      "retries" -> "3",
      "batch.size" -> "16384",
      "linger.ms" -> "1",
      "buffer.memory" -> "33554432",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> classOf[org.apache.eagle.dataproc.impl.storm.kafka.JsonSerializer].getCanonicalName
    ).asJava)
    props
  }

  @transient var initialized:AtomicBoolean = new AtomicBoolean(false)
  @transient var producer:KafkaProducer[String,AnyRef] = null
  @transient var topic:String = null
  @transient var timeoutMs:Long = 3000

  val LOG = LoggerFactory.getLogger(classOf[KafkaSinkExecutor])

  private def init():Unit = {
    if(this.initialized != null && this.initialized.get()){
      LOG.info("Already initialized, skip")
      return
    }
    this.initialized = new AtomicBoolean(false)
    if (producer != null) {
      LOG.info(s"Closing $producer")
      producer.close()
    }
    LOG.info("Initializing and creating Kafka Producer")
    if (config.contains(TOPIC_KEY)) {
      this.topic = config.get(TOPIC_KEY).get.asInstanceOf[String]
    } else {
      throw new IllegalStateException("topic is not defined")
    }
    val props = getDefaultProps
    props.putAll((config - TOPIC_KEY).asJava)
    producer = new KafkaProducer[String, AnyRef](props)
    LOG.info(s"Created new KafkaProducer: $producer")
    initialized.set(true)
  }

  override def apply(value: AnyRef): Unit = {
    if(initialized == null || !initialized.get()) init()
    if(topic == null) throw new IllegalStateException("topic is not defined")
    val isList = value.isInstanceOf[java.util.List[AnyRef]]
    val record: ProducerRecord[String, AnyRef] = if(isList){
      val list = value.asInstanceOf[java.util.List[AnyRef]]
      if(list.size() == 1) {
        new ProducerRecord[String, AnyRef](topic, value.asInstanceOf[java.util.List[AnyRef]].get(0))
      }else{
        new ProducerRecord[String, AnyRef](topic, value)
      }
    }else{
      new ProducerRecord[String, AnyRef](topic,value)
    }
    producer.send(record,new Callback(){
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if(exception!=null){
          LOG.error(s"Failed to send record $value to topic: $topic",exception)
        }
      }
    })
  }
}