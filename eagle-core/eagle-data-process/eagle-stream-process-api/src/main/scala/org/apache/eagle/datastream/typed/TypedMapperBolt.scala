package org.apache.eagle.datastream.typed

import backtype.storm.tuple.Tuple

/**
 * @since  12/1/15
 */
case class TypedMapperBolt[I,O](fn: I => O) extends TypedBaseBolt[I,O]{
  override def exec(value: I)(implicit input: Tuple): Unit = {
    emit(fn(value))
  }
}