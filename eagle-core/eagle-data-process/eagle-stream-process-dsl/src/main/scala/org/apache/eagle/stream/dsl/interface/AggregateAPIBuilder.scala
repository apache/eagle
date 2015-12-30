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

import org.apache.eagle.stream.dsl.definition.StreamDefinition

private case class AggregateContext(from:StreamDefinition,var to:StreamDefinition)

trait AggregateAPIBuilder extends AbstractAPIBuilder{
  private var _context:AggregateContext = null

  onInit {
      _context = null
  }

  def aggregate(stream:(String,String)):AggregateAPIBuilder= {
    shouldNotBeNull(_context)
    if(stream._1.equals(stream._2)) throw new IllegalArgumentException(s"input and output stream should not be same but it's ${stream._1} -> ${stream._2}")
    _context = AggregateContext(context.getStreamManager.getStreamDefinition(stream._1),StreamDefinition(stream._1,null))
    this
  }

  def using(rule:ScriptString):StreamSettingAPIBuilder = ???
//  rule match {
//    case sql:SqlScript => {
//      val producer = null // TODO: _context.from.getProducer.aggregateBy(sql.getContent)
//      _context.to.setProducer(producer)
//      StreamSettingAPIBuilder(_context.to)
//    }
//    case _ => throw new UnsupportedOperationException(s"aggregate rule $rule is not supported")
//  }
}