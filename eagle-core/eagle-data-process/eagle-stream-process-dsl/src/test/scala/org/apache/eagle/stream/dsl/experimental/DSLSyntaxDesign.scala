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


object DefineInterface{
  trait DefineDef {
    def as(streamAttributes:(String,Symbol)*):DefineDef = as(StreamDef(streamAttributes))
    def as(streamDef:StreamDef):DefineDef
    def from (source:StreamMeta):DefineDef=null
  }
  case class StreamDef(attributes:Seq[(String,Symbol)])
  def define(name:String):DefineDef = null
}

object AlertInterface {
  trait PolicyDef extends StreamMeta
  case class SQLPolicyDef(policies:String*) extends PolicyDef
  class AlertDef{
    def by(policy:PolicyDef):AlertDef = null
    def by(policy:String):AlertDef = by(SQLPolicyDef(policy))
    def by(policy:SQLString):AlertDef = by(SQLPolicyDef(policy.sql))
    def parallism(parallismNum:Int):AlertDef = null
    def partitionBy(fields:String*):AlertDef = null
  }

  def alert:AlertDef=null

  case class MailNotificationStreamMetaDef(from:String,to:String,smtp:String,template:String) extends StreamMeta
  def mail(from:String,to:String,smtp:String,template:xml.Elem) = MailNotificationStreamMetaDef(from,to,smtp,template.toString)
}

object AggregateInterface{
  trait AggregateRuleDef extends StreamMeta
  case class SQLAggregateRuleDef(rules:String) extends AggregateRuleDef
  trait AggregateDef{
    def by(rule:AggregateRuleDef):AggregateDef = null
    def by(sql:String):AggregateDef = by(SQLAggregateRuleDef(sql))
    def by(sql:SQLString):AggregateDef = by(SQLAggregateRuleDef(sql.sql))
    def parallism(parallismNum:Int):AggregateDef = null
    def partitionBy(fields:String*):AggregateDef = null
  }
  case class SQLAggregateDef(sql:String*) extends AggregateDef
  def aggregate:AggregateDef = null
}

object StreamInterface{
  trait StreamMeta extends Serializable
  implicit class NameInterface(name:String) {
    def to(toName:String): NameInterface = null
    def ~>(toName:String): NameInterface = to(toName)
    def to(streamMeta: StreamMeta): NameInterface = null
    def ~>(streamMeta: StreamMeta): NameInterface = to(streamMeta)
    def where(filter:String): NameInterface = null
    def partitionBy(fields:String*): NameInterface = null
  }
}

object KafkaInterface{
  case class KafkaStreamMetaDef(topic:String,zk:String) extends StreamMeta
  def kafka(topic:String,zk:String = "localhost:2181"):KafkaStreamMetaDef = KafkaStreamMetaDef(topic,zk)
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
    kafka(topic="metricStream_1",zk=conf"kafka.zk.hosts")
  define ("metricStream_2") as ("name" -> 'string, "value"->'double, "timestamp"->'long) from
    kafka(topic="metricStream_2")

  alert partitionBy "metricStream_1.metricType" parallism 1 by sql"""
    from metricStream_1[component=='dn' and metricType=="RpcActivityForPort50020.RpcQueueTimeNumOps"].time[3600]
    select sum(value) group by host output every 1 hour insert into alertStream;
    """

  aggregate  partitionBy "metricStream_1.metricType" parallism 2 by sql"""
    from metricStream_1[component=='dn' and metricType=="RpcActivityForPort50020.RpcQueueTimeNumOps"].time[3600]
    select sum(value) group by host output every 1 hour insert into aggregatedMetricStream_1;
    """

  "alertStream" ~> kafka("alert_topic",zk=conf"kafka.zk.hosts")
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
  "aggregatedMetricStream_1" to kafka("aggregated_stream_dn") where "component == 'dn'" partitionBy "aggregatedMetricStream_1.metricType"
  "aggregatedMetricStream_1" ~> druid("aggregated_stream_nn")  where "component == 'nn'" partitionBy "aggregatedMetricStream_1.metricType"
  // # end
}