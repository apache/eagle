package org.apache.eagle.datastream.storm

import backtype.storm.tuple.Tuple

/**
 * @since  12/1/15
 */
case class KeyedStreamFilterBolt[V](fn:V => Boolean) extends KeyedStreamBolt[V,V]{
  override def exec(value: V)(implicit input: Tuple) ={
    if(fn(value)){
      emit(value)
    }
  }
}