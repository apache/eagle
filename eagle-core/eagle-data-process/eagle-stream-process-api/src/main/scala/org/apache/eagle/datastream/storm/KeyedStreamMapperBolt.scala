package org.apache.eagle.datastream.storm

import backtype.storm.tuple.Tuple

/**
 * @since  12/1/15
 */
case class KeyedStreamMapperBolt[I,O](fn: I => O) extends KeyedStreamBolt[I,O]{
  override def exec(value: I)(implicit input: Tuple): Unit = {
    emit(fn(value))
  }
}