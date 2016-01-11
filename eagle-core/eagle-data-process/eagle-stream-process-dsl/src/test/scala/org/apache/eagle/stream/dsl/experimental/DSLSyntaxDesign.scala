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
package org.apache.eagle.stream.dsl.experimental

import org.apache.eagle.stream.dsl.experimental.StreamInterface._
import org.apache.eagle.stream.dsl.experimental.StringPrefix._
import org.apache.eagle.stream.dsl.experimental.AlertInterface._
import org.apache.eagle.stream.dsl.experimental.AggregateInterface._
import org.apache.eagle.stream.dsl.experimental.DefineInterface._
import org.apache.eagle.stream.dsl.experimental.FilterInterface._

import scala.util.matching.Regex

object DefineInterface{
  trait DefineDef {
    def as(streamAttributes:(String,Symbol)*):DefineDef = as(StreamDef(streamAttributes))
    def as(streamDef:StreamDef):DefineDef
    def from (source:StreamMeta):DefineDef = ???
  }
  case class StreamDef(attributes:Seq[(String,Symbol)])
  def define(name:String):DefineDef = ???
  def define(name:Symbol):DefineDef = define(name.toString())

  def stream:DefineDef = ???

  def datetime(format:String = "YYYY-MM-DD hh:mm"):DefineDef = ???
}

object AlertInterface {
  trait PolicyDef extends StreamMeta
  case class SQLPolicyDef(policies:String*) extends PolicyDef
  class AlertDef{
    def by(policy:PolicyDef):AlertDef = ???
    def by(policy:String):AlertDef = by(SQLPolicyDef(policy))
    def by(policy:SQLString):AlertDef = by(SQLPolicyDef(policy.sql))
    def parallism(parallismNum:Int):AlertDef = ???
    def partitionBy(fields:String*):AlertDef = ???
  }

  def alert:AlertDef = ???

  case class MailNotificationStreamMetaDef(from:String,to:String,smtp:String,template:String) extends StreamMeta
  def mail(from:String,to:String,smtp:String,template:xml.Elem) = MailNotificationStreamMetaDef(from,to,smtp,template.toString)
}

object AggregateInterface{
  trait AggregateRuleDef extends StreamMeta
  case class SQLAggregateRuleDef(rules:String) extends AggregateRuleDef
  trait AggregateDef{
    def by(rule:AggregateRuleDef):AggregateDef = ???
    def by(sql:String):AggregateDef = by(SQLAggregateRuleDef(sql))
    def by(sql:SQLString):AggregateDef = by(SQLAggregateRuleDef(sql.sql))
    def parallism(parallismNum:Int):AggregateDef = ???
    def partitionBy(fields:String*):AggregateDef = ???
  }
  case class SQLAggregateDef(sql:String*) extends AggregateDef
  def aggregate:AggregateDef = ???
}

object FilterInterface{
  trait Collector{
    def collect(tuple:AnyRef)
  }
  trait FilterDef{
    def by[T1<:AnyRef](func:(T1,Collector) => Any):FilterDef = this
    def by(regEx:Regex):FilterDef = this
    def inStream:String
    def outStream:String
    def as(streamAttributes:(String,Any)*):FilterDef = this
  }

  def filter(stream:String):FilterDef = new FilterDef(){
    override def inStream: String = stream
    override def outStream: String = stream
  }
  def filter(streamFromTo:(String,String)):FilterDef = new FilterDef(){
    override def inStream: String = streamFromTo._1
    override def outStream: String = streamFromTo._2
  }
}

object StreamInterface{
  trait StreamMeta extends Serializable
  implicit class StreamNameImplicits(name:String) {
    def to(toName:String): StreamNameImplicits = ???
    def ~>(toName:String): StreamNameImplicits = to(toName)
    def to(streamMeta: StreamMeta): StreamNameImplicits = ???
    def ~>(streamMeta: StreamMeta): StreamNameImplicits = to(streamMeta)
    def :=>(streamMeta: StreamMeta): StreamNameImplicits = to(streamMeta)
    def where(filter:String): StreamNameImplicits = ???
    def partitionBy(fields:String*): StreamNameImplicits = ???
    def as(attributes:(String,Symbol)*) = ???
    def :=(func: => Any) :DefineDef = ???
  }

  implicit class SymbolStreamNameImplicits(name:Symbol) extends StreamNameImplicits(name.toString())
}

object KafkaInterface{
  case class KafkaStreamMetaDef(topic:String,zk:String) extends StreamMeta
  def kafka(topic:String,zk:String = "localhost:2181",deserializer:String="JsonDeserializer"):KafkaStreamMetaDef = KafkaStreamMetaDef(topic,zk)
}

object DruidInterface{
  case class DruidStreamMetaDef(datasource:String,zk:String) extends StreamMeta
  def druid(datasource:String,zk:String = "localhost:2181"):DruidStreamMetaDef = DruidStreamMetaDef(datasource,zk)
}

object StringPrefix{
  case class SQLString(sql:String) extends Serializable
  implicit class SQLStringInterpolation(val sc:StringContext) extends AnyVal{
    def sql(arg:Any):SQLString = SQLString(arg.asInstanceOf[String])
  }
  implicit class ConfigKeyInterface(val sc:StringContext)  extends AnyVal{
    def c[T](arg:Any):T = ???
    def conf[T](arg:Any):T = ???
  }
}

//////////////////////////////////////
// Sample Application               //
//////////////////////////////////////

// Declarative DSL-Based Application by extension or script
object SampleApp {
  // Plug-able DSL Interface

  // Topology Definition API by extends or script
  import org.apache.eagle.stream.dsl.experimental.KafkaInterface._
  import org.apache.eagle.stream.dsl.experimental.DruidInterface._

  // #!/bin/bash
  // exec scala "$0" "$@"
  // !#
  // # start
  define ("metricStream_1") as ("name" -> 'string, "value"->'double, "timestamp"->'long) from
    kafka(topic="metricStream_1",zk=conf"kafka.zk.hosts",deserializer="")

  define ("metricStream_2") as ("name" -> 'string, "value"->'double, "timestamp"->'long) from
    kafka(topic="metricStream_2")

  define ("logStream_3") from kafka(topic="logStream_3",zk = "localhost:2181")

  // filter by function
  filter ("logStream_3") by {(line:String,collector) => collector.collect(line)} as ("name" -> 'string, "value"->'double, "timestamp"->'long)
  // "logStream_3" as ("name" -> 'string, "value"->'double, "timestamp"->'long)

  // filter by pattern and rename stream
  filter("logStream_3"->"logStream_3_parsed") by """(?<timestamp>\d{4}-\d{2}-\d{2})""".r as ("name" -> 'string, "value"->'double, "timestamp"-> datetime("YYYY-MM-DD"))

  alert partitionBy "metricStream_1.metricType" parallism 1 by { sql"""
    from metricStream_1[component=='dn' and metricType=="RpcActivityForPort50020.RpcQueueTimeNumOps"].time[3600]
    select sum(value) group by host output every 1 hour insert into alertStream;
  """ }

  aggregate partitionBy "metricStream_1.metricType" parallism 2 by { sql"""
    from metricStream_1[component=='dn' and metricType=="RpcActivityForPort50020.RpcQueueTimeNumOps"].time[3600]
    select sum(value) group by host output every 1 hour insert into aggregatedMetricStream_1;
  """ }

  aggregate partitionBy "metricStream_1.metricType" parallism 2 by { sql"""
    from metricStream_1[component=='dn' and metricType=="RpcActivityForPort50020.RpcQueueTimeNumOps"].time[3600]
    select sum(value) group by host output every 1 hour insert into aggregatedMetricStream_2;
  """ }

  'alertStream ~> kafka("alert_topic",zk=conf"kafka.zk.hosts")
  "alertStream" to mail(
    from = "sender@eagle.incubator.apache.org",
    to = "receiver@eagle.incubator.apache.org",
    smtp = "localhost:25",
    template =
      <html>
        <head>
        <title>Alert Notification</title>
        </head>
        <body>
          <h1>Message</h1>
          <p>$message</p>
        </body>
      </html>
  )

  // split stream by logic
  'aggregatedMetricStream_1 to kafka("aggregated_stream_dn") where "component == 'dn'" partitionBy "aggregatedMetricStream_1.metricType"
  'aggregatedMetricStream_1 ~> druid("aggregated_stream_nn")  where "component == 'nn'" partitionBy "aggregatedMetricStream_1.metricType"
  // # end

//  LogStash Pipeline Support
//  =========================
//
//  define("logstream") from logstash"""
//      input{
//
//      }
//  """
//  filter("logstream") by logstash"""
//    filter{
//
//    }
//  """
//  "logstream" -> logstash"""
//    filter{
//
//    }
//  """
//  logstash("""
//    input{
//
//    }
//    filter{
//
//    }
//    output{
//
//    }
//  """)

  "streamA" :=
    aggregate partitionBy "metricType" parallism 2 by { sql"""
    from metricStream_1[component=='dn' and metricType=="RpcActivityForPort50020.RpcQueueTimeNumOps"].time[3600]
    select sum(value) group by host output every 1 hour insert into aggregatedMetricStream_1;
    """ }
  "streamB" :=
    stream from kafka(topic="metricStream_1",zk=conf"kafka.zk.hosts",deserializer="") as ("name" -> 'string, "value"->'double, "timestamp"->'long)
  "streamC" :=>
    kafka("alert_topic",zk=conf"kafka.zk.hosts") partitionBy "someField"
}