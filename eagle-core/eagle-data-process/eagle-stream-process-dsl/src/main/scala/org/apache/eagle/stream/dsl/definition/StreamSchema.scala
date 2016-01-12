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

class Attribute(name:String) extends Serializable{
  def getName:String = name
}

case class StringAttribute(name:String) extends Attribute(name)
case class LongAttribute(name:String) extends Attribute(name)
case class IntegerAttribute(name:String) extends Attribute(name)
case class BooleanAttribute(name:String) extends Attribute(name)
case class FloatAttribute(name:String) extends Attribute(name)
case class DoubleAttribute(name:String) extends Attribute(name)
case class DatetimeAttribute(name:String,format:String) extends Attribute(name)

object Attribute{
  def string(name:String) = StringAttribute(name)
  def long(name:String) = LongAttribute(name)
  def integer(name:String) = IntegerAttribute(name)
  def boolean(name:String) = BooleanAttribute(name)
  def float(name:String) = FloatAttribute(name)
  def double(name:String) = DoubleAttribute(name)
  def datetime(name:String)(format:String) = DatetimeAttribute(name,format)

  def apply(name:String,typeName:String):Attribute = typeName match {
    case "string" => string(name)
    case "long" => long(name)
    case "integer" => integer(name)
    case "boolean" => boolean(name)
    case "float" => float(name)
    case "double" => double(name)
    case _ => throw new UnsupportedOperationException(s"""Unknown attribute type $typeName for attribute "$name"""")
  }
}

case class StreamSchema(name:String,attributes:Seq[Attribute]) extends Serializable{
  def getAttribute(attributeName:String):Option[Attribute]={
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

object StreamSchema{
  /**
   *
   * @param name stream name
   * @param attributes support string, symbol, Attribute and so on.
   * @return
   */
  def build(name:String,attributes:Seq[AnyRef]):StreamSchema = {
    StreamSchema(name,attributes.map{ a:AnyRef =>
      a match {
        case t:(String,AnyRef) => {
          t._2 match {
            case v:String => Attribute(t._1,v)
            case v:Symbol => Attribute(t._1,v.name)
            case _ => throw new UnsupportedOperationException(s"Illegal attribute definition $name -> $a")
          }
        }
        case t:Attribute => t
        case _ => throw new UnsupportedOperationException(s"Illegal attribute definition $name -> $a")
      }
    })
  }
}

class StreamUndefinedException(message:String = "stream is not defined",throwable: Throwable = null) extends Exception(message,throwable)