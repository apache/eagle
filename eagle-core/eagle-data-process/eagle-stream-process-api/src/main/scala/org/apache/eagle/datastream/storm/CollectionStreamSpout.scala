package org.apache.eagle.datastream.storm

import java.util

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import org.apache.eagle.datastream.{StreamInfo, OutputFieldNameConst}

import scala.collection.JavaConverters._

/**
 * @since  12/6/15
 */
case class CollectionStreamSpout(seq:Seq[Any])(implicit info:StreamInfo[Any]) extends BaseRichSpout{
  val _seq = seq
  var _currentIndex:Int=0
  var _collector:SpoutOutputCollector=null
  var _seqLen = seq.length

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    this._currentIndex = 0
    this._collector = collector
  }

  override def nextTuple(): Unit = {
    val current = _seq(_currentIndex)
    if(info.outKeyed) {
      _collector.emit(List(info.keySelector.key(current),current).asJava.asInstanceOf[util.List[AnyRef]])
    }else{
      _collector.emit(List(current).asJava.asInstanceOf[util.List[AnyRef]])
    }
    if(_currentIndex >= _seqLen - 1) {
      _currentIndex = 0
    }else{
      _currentIndex += 1
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