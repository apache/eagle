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
package org.apache.eagle.stream.dsl.utils

import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.eagle.stream.dsl.definition.StreamDefinition

import scala.collection.mutable
import scala.util.matching.Regex
import scala.collection.JavaConverters._

object UtilImplicits {
  implicit class RegexImplicits(val regex:Regex) extends AnyVal{
    /**
     * """(?<ip>\d+\.\d+\.\d+\.\d+)\s+(?<method>\w+)\s+(?<path>[\w/\.]+)\s+(?<bytes>\d+)\s+(?<time>[\d\.]+)""".r
     * -> ("ip","method","path","bytes","time")
     *
     * @return pattern embedded named groups
     */
    def namedGroups:mutable.Map[String,Int] = FieldUtils.readField(regex.pattern,"namedGroups",true).asInstanceOf[java.util.Map[String,Int]].asScala
  }

  implicit class RegexMatchImplicits(val value:Regex.Match) extends AnyVal{
    def namedGroupsValue(regex:Regex):mutable.Map[String,String] = {
      val valueMap = mutable.ListMap[String,String]()
      regex.namedGroups.foreach(pair =>{
        valueMap(pair._1)=value.group(pair._2)
      })
      valueMap
    }
  }

  implicit class ObjectSeqImplicits(val input: Seq[AnyRef]) extends AnyVal{
    def asMap(stream:StreamDefinition):mutable.Map[String,AnyRef]={
      val result = mutable.ListMap[String,AnyRef]()
      var index = 0
      stream.getSchema.attributes.foreach(a =>{
        result(a.getName) = input(index)
        index += 1
      })
      result
    }
  }
}