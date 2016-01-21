package org.apache.eagle.stream.pipeline.extension

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.config.ConfigFactory
import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider
import org.apache.eagle.datastream.core._
import org.apache.eagle.partition.PartitionStrategy
import org.apache.eagle.stream.pipeline.parser.Processor
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

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


object ModuleManager{
  def getModuleMapperByType(moduleType:String):ModuleMapper = {
    classOfProcessorMapping(moduleType)
  }

  def findModuleType(moduleType:String):Boolean = classOfProcessorMapping.contains(moduleType)

  val classOfProcessorMapping = Map[String,ModuleMapper](
    "KafkaSource" -> KafkaSourceStreamProducer,
    "KafkaSink" -> KafkaSinkStreamProducer,
    "Alert" -> AlertStreamProducer,
    "Persistence" -> PersistProducer,
    "Aggregator" -> AggregatorProducer,
    "Console" -> ConsoleStreamProducer
  )
}

trait ModuleMapper{
  def getType:String
  def map(module:Processor):StreamProducer[Any]
}
object KafkaSourceStreamProducer extends ModuleMapper{
  def getType = "KafkaSource"
  override def map(module:Processor): StreamProducer[Any] = {
    val config = module.getConfig
    new StormSourceProducer[Any](new KafkaSourcedSpoutProvider(null).getSpout(ConfigFactory.parseMap(config.asJava)))
  }
}
object KafkaSinkStreamProducer extends ModuleMapper{
  def getType = "KafkaSink"
  override def map(module:Processor): StreamProducer[Any] = {
    val config = module.getConfig
    ForeachProducer[AnyRef](KafkaSinkExecutor(config))
  }
}
object ConsoleStreamProducer extends ModuleMapper{
  override def getType: String = "Stdout"
  override def map(module:Processor): StreamProducer[Any] = ForeachProducer[Any](m=>print(s"$m\n"))
}
object AlertStreamProducer extends ModuleMapper{
  def getType:String = "Alert"
  override def map(module:Processor): StreamProducer[Any] = {
    val config = module.getConfig
    val moduleId = module.getId
    // Support create functional AlertStreamProducer constructor
    new AlertStreamProducer (
      upStreamNames = config.getOrElse("upStreamNames",if(module.inputIds!=null) module.inputIds.asJava else null).asInstanceOf[java.util.List[String]],
      alertExecutorId = config.getOrElse("alertExecutorId",moduleId).asInstanceOf[String],
      consume = config.getOrElse("consume",true).asInstanceOf[Boolean],
      strategy = config.get("strategy") match {case Some(strategy)=> Class.forName(strategy.asInstanceOf[String]).newInstance().asInstanceOf[PartitionStrategy] case None => null}
    )
  }
}

object PersistProducer extends ModuleMapper{
  override def getType = "Persistence"
  override def map(module:Processor): StreamProducer[Any] = {
    val config = module.getConfig
    new PersistProducer(config.getOrElse("executorId",module.getId).asInstanceOf[String],StorageType.withName(config.getOrElse("storageType",null).asInstanceOf[String]))
  }
}

object AggregatorProducer extends ModuleMapper{
  override def getType: String = "Aggregator"
  override def map(module:Processor): StreamProducer[Any] = {
    val config = module.getConfig
    new AggregateProducer(
      upStreamNames = config.getOrElse("upStreamNames",if(module.inputIds!=null) module.inputIds.asJava else null).asInstanceOf[java.util.List[String]],
      config.getOrElse("analyzer",module.getId).asInstanceOf[String],
      config.get("sql") match {case Some(sql) => sql.asInstanceOf[String] case None => null },
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