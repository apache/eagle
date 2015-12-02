package org.apache.eagle.datastream.typed

import backtype.storm.tuple.Tuple

/**
 * @since  12/1/15
 */
case class TypedFilterBolt[V](fn:V => Boolean) extends TypedBaseBolt[V,V]{
  override def exec(value: V)(implicit input: Tuple) ={
    if(fn(value)){
      emit(value)
    }
  }
}