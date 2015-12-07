package org.apache.eagle.datastream.storm

import java.util

import backtype.storm.tuple.Tuple
import org.apache.eagle.datastream.core.StreamInfo

/**
 * @since  12/6/15
 */
case class ForeachBoltWrapper(fn:Any=>Unit)(implicit info:StreamInfo) extends AbstractStreamBolt[Any]  {
  /**
   * Handle keyed stream value
   * @param value
   */
  override def onKeyValue(key:Any,value: Any)(implicit input:Tuple): Unit = {
    fn(value)
  }

  /**
   * Handle non-keyed stream values list
   *
   * @param values
   */
  override def onValues(values: util.List[AnyRef])(implicit input:Tuple): Unit = {
    fn(values)
  }
}