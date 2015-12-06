package org.apache.eagle.datastream.storm

import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import org.apache.eagle.datastream.{StreamInfo, OutputFieldNameConst}
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

/**
 * @since  12/6/15
 */
case class IterableStreamSpout(iterable: Iterable[Any],recycle:Boolean = true)(implicit info:StreamInfo[Any]) extends BaseRichSpout {
  val LOG = LoggerFactory.getLogger(classOf[IterableStreamSpout])
  var _collector:SpoutOutputCollector=null
  var _iterator:Iterator[Any] = null

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    this._collector = collector
    this._iterator = iterable.iterator
  }

  override def nextTuple(): Unit = {
    if(_iterator.hasNext){
      val current = _iterator.next().asInstanceOf[AnyRef]
      if(info.outKeyed) {
        _collector.emit(List(info.keySelector.key(current),current).asJava.asInstanceOf[util.List[AnyRef]])
      }else{
        _collector.emit(List(current).asJava)
      }
    }else if(recycle){
      LOG.info("Recycling the iterator")
      _iterator = iterable.iterator
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    if(info.outKeyed) {
      declarer.declare(new Fields(OutputFieldNameConst.FIELD_KEY,OutputFieldNameConst.FIELD_VALUE))
    }else{
      declarer.declare(new Fields(OutputFieldNameConst.FIELD_PREFIX))
    }
  }
}
