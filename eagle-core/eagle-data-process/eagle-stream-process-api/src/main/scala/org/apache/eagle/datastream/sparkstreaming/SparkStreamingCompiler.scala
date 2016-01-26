package org.apache.eagle.datastream.sparkstreaming

import com.typesafe.config.Config
import org.apache.eagle.datastream.core.{StreamProducer, AbstractTopologyExecutor, AbstractTopologyCompiler, StreamProducerGraph}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.slf4j.LoggerFactory


/**
  * Created by zqin on 2016/1/21.
  */
case class SparkStreamingCompiler (config: Config, graph: StreamProducerGraph) extends AbstractTopologyCompiler{
  override def buildTopology: AbstractTopologyExecutor ={
    val LOG = LoggerFactory.getLogger(SparkStreamingCompiler.getClass)
    val appName = config.getString("envContextConfig.topologyName")
    val conf = new SparkConf().setAppName(appName)
    val localMode: Boolean = config.getString("envContextConfig.mode").equalsIgnoreCase("local")
   // val dStreamsCollection = scala.collection.mutable.Map[StreamProducer[Any], DStream]
    if (!localMode) {
      LOG.info("Running in cluster mode")

    } else {
      LOG.info("Running in local mode")
      conf.setMaster("spark://localhost:7077")
    }

    val ssc = new StreamingContext(conf, Seconds(1))
    val iter = graph.iterator()
    while(iter.hasNext){
      val from = iter.next()
      if(graph.isSource(from)){
        val dStream = DStreamFactory.createInputDStream(ssc,from)
        val edges = graph.outgoingEdgesOf(from)
        edges.foreach(sc => {
          val producer = graph.getNodeByName(sc.to.name)
          producer match {
            case Some(p) => {
              if (dStream.isInstanceOf[DStream[Any]]) {
                DStreamFactory.createDStreamsByDFS(graph, dStream.asInstanceOf[DStream[Any]], p)
              }
              else {
                throw throw new IllegalArgumentException("Can not create DStream from " + dStream.toString)
              }
            }
            case None => throw new IllegalArgumentException("please check bolt name " + sc.to.name)
          }
        })
      }
    }
    new SparkStreamingExecutorImpl(ssc, config)
  }
}
