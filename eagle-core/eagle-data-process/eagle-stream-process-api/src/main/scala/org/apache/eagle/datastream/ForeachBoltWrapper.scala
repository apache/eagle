package org.apache.eagle.datastream

import java.util

import backtype.storm.tuple.Tuple
import org.apache.eagle.datastream.storm.AbstractStreamBolt

/**
 * @since  12/6/15
 */
case class ForeachBoltWrapper(fn:Any=>Unit)(implicit info:StreamInfo[Any]) extends AbstractStreamBolt[Any]  {
  /**
   * Handle keyed stream value
   * @param value
   */
  override def handleKeyValue(key:Any,value: Any)(implicit input:Tuple): Unit = {
    fn(value)
  }

  /**
   * Handle non-keyed stream values list
   *
   * @param values
   */
  override def handleValues(values: util.List[AnyRef])(implicit input:Tuple): Unit = {
    fn(values)
  }
}