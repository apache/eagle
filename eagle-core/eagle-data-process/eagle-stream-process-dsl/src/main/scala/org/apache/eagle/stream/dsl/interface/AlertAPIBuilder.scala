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
package org.apache.eagle.stream.dsl.interface

import org.apache.eagle.alert.entity.AlertAPIEntity
import org.apache.eagle.datastream.core.StreamProducer
import org.apache.eagle.stream.dsl.definition.StreamDefinition

private case class AlertContext(from:StreamDefinition,to:StreamDefinition)

trait AlertAPIBuilder extends AbstractAPIBuilder with ConnectAPIBuilder{
  private var _context: AlertContext = null
  def alert(alertExecutor:String):StreamProducer[AlertAPIEntity]={
    shouldNotBeNull(primaryStream)
    primaryStream.getProducer.alert(Seq(primaryStream.name),alertExecutor)
  }

//  def alert(policy:ScriptString):ScriptAlertAPIBuilder = ???
  def alert(flowTo:(String,String)):AlertAPIBuilder = {
    _context = AlertContext(context.getStreamManager.getStreamDefinition(flowTo._1),context.getStreamManager.getStreamDefinition(flowTo._2))
    this.primaryStream = _context.from
    this
  }

  def by(policy:Any):StreamSettingAPIBuilder = {
    policy match {
      case sql:SqlScript => {
        primaryStream.getProducer.alertByPolicy(sql.content)
      }
      case _ => throw new IllegalArgumentException(s"Policy $policy is not supported yet")
    }
    StreamSettingAPIBuilder(this.primaryStream)
  }
}

