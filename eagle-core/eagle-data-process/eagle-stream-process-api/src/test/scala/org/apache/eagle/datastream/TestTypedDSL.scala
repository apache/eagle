package org.apache.eagle.datastream

import org.apache.eagle.dataproc.impl.storm.kafka.KafkaSourcedSpoutProvider

/**
 * @author Chen, Hao (hchen9@ebay.com)
 * @since  12/4/15
 */
object TestTypedDSL extends App{
  val env = ExecutionEnvironments.getStorm
  env.config
  env.from(new KafkaSourcedSpoutProvider)

}
