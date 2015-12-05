package org.apache.eagle.datastream.storm

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.{Fields, Tuple}
import org.apache.eagle.datastream.OutputFieldNameConst

/**
 * @since  12/1/15
 */
abstract class KeyedStreamBolt[I,O](var keyer: Option[O => AnyRef] = None) extends BaseRichBolt{
  implicit var collector: OutputCollector = null

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext, collector: OutputCollector): Unit = {
    this.collector = collector
  }

  override def execute(input: Tuple): Unit = {
    implicit val _input = input
    input.size() match {
      case 1 => exec(input.getValue(0).asInstanceOf[I])
      case 2 => exec(input.getValue(1).asInstanceOf[I])
      case _ => throw new IllegalArgumentException(s"Support message in format (value) or (key,value), but got: $input ")
    }
  }

  def exec(value:I)(implicit input: Tuple)

  def emit(value:O)(implicit input:Tuple){
    if(keyer.isEmpty) {
      collector.emit(input, util.Arrays.asList(value.asInstanceOf[AnyRef]))
    }else{
      collector.emit(input,util.Arrays.asList(keyer.get.apply(value),value.asInstanceOf[AnyRef]))
    }
    collector.ack(input)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer) {
    if(keyer.isEmpty){
      declarer.declare(new Fields(OutputFieldNameConst.FIELD_VALUE))
    }else {
      declarer.declare(new Fields(OutputFieldNameConst.FIELD_KEY,OutputFieldNameConst.FIELD_VALUE))
    }
  }
}