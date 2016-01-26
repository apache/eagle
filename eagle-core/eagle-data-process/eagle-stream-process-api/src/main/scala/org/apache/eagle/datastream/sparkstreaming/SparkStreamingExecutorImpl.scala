package org.apache.eagle.datastream.sparkstreaming

import org.apache.eagle.datastream.core.AbstractTopologyExecutor
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

/**
  * Created by zqin on 2016/1/21.
  */
case class SparkStreamingExecutorImpl(ssc: StreamingContext, config: com.typesafe.config.Config) extends AbstractTopologyExecutor {
  val LOG = LoggerFactory.getLogger(classOf[SparkStreamingExecutorImpl])
  @throws(classOf[Exception])
  def execute {
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
