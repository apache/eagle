package org.apache.eagle.stream.dsl.definition

import org.apache.eagle.datastream.core.StreamProducer

/**
 * @param name stream name
 */
case class StreamDefinition(name:String,var schema:StreamSchema = null) extends Serializable{
  private var streamProducer:StreamProducer[Any] = null
  def setSchema(schema: StreamSchema): Unit = this.schema = schema
  def getSchema: StreamSchema = this.schema
  def setProducer(producer:StreamProducer[Any]) = {
    this.streamProducer = producer
  }
  def getProducer = this.streamProducer
}