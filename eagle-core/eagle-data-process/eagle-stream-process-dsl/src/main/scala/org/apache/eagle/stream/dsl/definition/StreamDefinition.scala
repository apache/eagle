package org.apache.eagle.stream.dsl.definition

import org.apache.eagle.datastream.core.StreamProducer

/**
 * @param name stream name
 */
case class StreamDefinition(name:String,var schema:StreamSchema = null) extends Serializable{
  private var streamProducer:StreamProducer[AnyRef] = null
  def setSchema(schema: StreamSchema): Unit = this.schema = schema
  def getSchema(name:String): StreamSchema = this.schema
  def setProducer(producer:StreamProducer[AnyRef]) = {
    this.streamProducer = producer
  }
  def getProducer = this.streamProducer
}