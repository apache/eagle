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
package org.apache.eagle.stream.dsl.definition

import scala.collection.mutable

class StreamDefinitionManager extends Serializable{
  /**
   * Map[StreamName -> StreamDefinition]
   */
  private val streamDefinitionMap = mutable.Map[String,StreamDefinition]()
  def getStreamDefinition(name:String):StreamDefinition = {
    streamDefinitionMap.get(name) match {
      case Some(definition) => definition
      case None => throw new IllegalAccessException(s"""Stream definition named "$name" is not found""")
    }
  }

  /**
   * Register stream definition
   *
   * @param name
   * @param definition
   */
  def setStreamDefinition(name:String,definition:StreamDefinition):Unit = {
    if(streamDefinitionMap.contains(name)){
      throw new IllegalStateException(s"Failed to register stream definition for $name, because stream definition of $name already exists")
    }else{
      streamDefinitionMap.put(name,definition)
    }
  }
  def init():Unit = {}
}