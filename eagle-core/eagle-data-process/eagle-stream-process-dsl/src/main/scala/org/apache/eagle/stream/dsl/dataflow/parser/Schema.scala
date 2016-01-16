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

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable

class Field(name:String) extends Serializable{
  def getName:String = name
}

case class StringField(name:String) extends Field(name)
case class LongField(name:String) extends Field(name)
case class IntegerField(name:String) extends Field(name)
case class BooleanField(name:String) extends Field(name)
case class FloatField(name:String) extends Field(name)
case class DoubleField(name:String) extends Field(name)
case class DatetimeField(name:String,format:String) extends Field(name)

object Field{
  def string(name:String) = StringField(name)
  def long(name:String) = LongField(name)
  def integer(name:String) = IntegerField(name)
  def boolean(name:String) = BooleanField(name)
  def float(name:String) = FloatField(name)
  def double(name:String) = DoubleField(name)
  def datetime(name:String)(format:String) = DatetimeField(name,format)

  def apply(name:String,typeName:String):Field = typeName match {
    case "string" => string(name)
    case "long" => long(name)
    case "integer" => integer(name)
    case "boolean" => boolean(name)
    case "float" => float(name)
    case "double" => double(name)
    case _ => throw new UnsupportedOperationException(s"""Unknown attribute type $typeName for attribute "$name"""")
  }
}

case class Schema(attributes:Seq[Field]) extends Serializable{
  def getAttribute(attributeName:String):Option[Field]={
    if(attributes != null){
      attributes.find(_.getName.eq(attributeName))
    }else None
  }

  def indexOfAttribute(attributeName:String):Int = {
    if(attributes != null){
      attributes.indexWhere(_.getName.eq(attributeName))
    } else -1
  }

  @throws[IllegalArgumentException]
  def indexOfAttributeOrException(attributeName:String):Int = {
    if(attributes != null){
      attributes.indexWhere(_.getName.eq(attributeName))
    } else throw new IllegalArgumentException(s"Attribute [$attributeName] is not found in stream $this")
  }
}

object Schema{
  def parse(map:Map[String,AnyRef]):Schema = {
    new Schema(map.keys.map {attributeName =>
      map(attributeName) match{
        case simpleType:String => Field(attributeName,simpleType)
        case complexType:java.util.Map[String,AnyRef] => throw new IllegalStateException(s"ComplexType attribute definition is supported yet [$attributeName : $complexType] ")
        case otherType@_ => throw new IllegalStateException(s"Illegal attribute definition $attributeName : $otherType")
      }
    }.toSeq)
  }

  /**
   * @param attributes support string, symbol, Attribute and so on.
   * @return
   */
  def build(attributes:Seq[AnyRef]):Schema = {
    new Schema(attributes.map{ a:AnyRef =>
      a match {
        case t:(String, AnyRef) => {
          t._2 match {
            case v:String => Field(t._1,v)
            case v:Symbol => Field(t._1,v.name)
            case _ => throw new UnsupportedOperationException(s"Illegal attribute definition $a")
          }
        }
        case t:Field => t
        case _ => throw new UnsupportedOperationException(s"Illegal attribute definition $a")
      }
    })
  }
}

private[dataflow] class StreamUndefinedException(message:String = "stream is not defined",throwable: Throwable = null) extends Exception(message,throwable)

private[dataflow] class SchemaSet {
  private val processorSchemaCache = mutable.Map[String,Schema]()
  def set(schemaId:String,schema:Schema):Unit = {
    if(processorSchemaCache.contains(schemaId)) throw new IllegalArgumentException(
      s"""
         |Failed to define schema for $schemaId as $schema,
         |because it has been defined as ${processorSchemaCache(schemaId)},
         |please call updateSchema(processorId,schema) instead
       """)
    processorSchemaCache.put(schemaId,schema)
  }
  def get(schemaId:String):Option[Schema] = processorSchemaCache.get(schemaId)
}

private[dataflow] object SchemaSet{
  def empty() = new SchemaSet()
  /**
   * For example:
   *
   * <code>
   *    {
   *      metricStream {
   *        metric: string
   *        value: double
   *        timestamp: long
   *      }
   *    }
   * </code>
   * @param schemaConfig
   * @return
   */
  def parse(schemaConfig:Map[String,AnyRef]):SchemaSet = {
    val schemas = new SchemaSet()
    schemaConfig.foreach(entry =>{
      schemas.set(entry._1,Schema.parse(entry._2.asInstanceOf[java.util.HashMap[String,AnyRef]].toMap))
    })
    schemas
  }

  def parse(config:Config):SchemaSet = parse(config.root().unwrapped().asInstanceOf[java.util.HashMap[String,AnyRef]].toMap)
}

