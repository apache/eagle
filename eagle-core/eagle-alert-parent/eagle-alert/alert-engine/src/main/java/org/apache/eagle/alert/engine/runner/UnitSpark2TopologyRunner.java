package org.apache.eagle.alert.engine.runner;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.spark.function.*;
import org.apache.eagle.alert.engine.spark.function2.AlertBoltSpark2Function;
import org.apache.eagle.alert.engine.spark.function2.AlertPublisherBoltSpark2Function;
import org.apache.eagle.alert.engine.spark.function2.CorrelationSpoutSpark2Function;
import org.apache.eagle.alert.engine.spark.function2.StreamRouteBoltSpark2Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Tuple2;
import scala.collection.*;
import scala.collection.Iterator;

import java.util.*;

/**
 * Created by root on 7/26/16.
 */
public class UnitSpark2TopologyRunner {

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkTopologyRunner.class);

    public final static String WINDOW_SECOND = "topology.window";
    public final static int DEFAULT_WINDOW_SECOND = 2;
    public final static String SPARK_EXECUTOR_CORES = "topology.core";
    public final static String SPARK_EXECUTOR_MEMORY = "topology.memory";
    public final static String alertBoltNamePrefix = "alertBolt";
    public final static String alertPublishBoltName = "alertPublishBolt";
    public final static String SPARK_EXECUTOR_INSTANCES = "topology.spark.executor.num"; //no need to set if you open spark.dynamicAllocation.enabled  see https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation
    public final static String LOCAL_MODE = "topology.localMode";
    public final static String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    public final static String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    public final static String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";
    public final static String CONSUMER_KAFKA_TOPIC = "topology.topics";
    public final static String WINDOW_DURATIONS = "topology.windowDurations";
    public final static String CHECKPOINT_DIRECTORY = "topology.checkpointDirectory";


    //  private final IMetadataChangeNotifyService metadataChangeNotifyService;
    private final Object lock = new Object();
    private String topologyId;
    private Config config;
    private SparkSession sparkSession;

    public void run() {
        buildTopology(sparkSession, config);
    }

    public UnitSpark2TopologyRunner(IMetadataChangeNotifyService metadataChangeNotifyService, Config config) throws InterruptedException {

        this.topologyId = config.getString("topology.name");
        this.config = config;

        long window = config.hasPath(WINDOW_SECOND) ? config.getLong(WINDOW_SECOND) : DEFAULT_WINDOW_SECOND;

        SparkSession.Builder builder = SparkSession.builder();
        builder.appName(topologyId);
        boolean localMode = config.getBoolean(LOCAL_MODE);
        if (localMode) {
            LOG.info("Submitting as local mode");
            builder.master("local[*]");
        }
        String sparkExecutorCores = config.getString(SPARK_EXECUTOR_CORES);
        String sparkExecutorMemory = config.getString(SPARK_EXECUTOR_MEMORY);
        builder.config("spark.executor.cores", sparkExecutorCores);
        builder.config("spark.executor.memory", sparkExecutorMemory);

        this.sparkSession = builder.getOrCreate();

    }


    private void buildTopology(SparkSession sparkSession, Config config) {

       /* String zkQuorum = config.getString("spout.kafkaBrokerZkQuorum");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", zkQuorum);
        kafkaParams.put("auto.offset.reset", "largest");//smallest|largest 重头消费|最新消费


        String topic = config.getString(CONSUMER_KAFKA_TOPIC);
        Set<String> topics = new HashSet<String>(Arrays.asList(topic));*/

        int windowDurations = config.getInt(WINDOW_DURATIONS);
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(PUBLISH_TASK_NUM);


        // JavaPairDStream<String, String> messages = null;
        //KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        Dataset<String> lines = sparkSession
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load().as(Encoders.STRING());

        Dataset<Tuple2<Integer, PartitionedEvent>> line = lines.flatMap(new CorrelationSpoutSpark2Function(numOfRouter, config), Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(PartitionedEvent.class)));

        Dataset<Tuple2<Integer, PartitionedEvent>> routblotResult = line.repartition(numOfRouter).mapPartitions(new StreamRouteBoltSpark2Function(config, "streamBolt"), Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(PartitionedEvent.class)));

        Dataset<Tuple2<String, AlertStreamEvent>> alertResult = routblotResult
                .repartition(numOfAlertBolts)
                .mapPartitions(new AlertBoltSpark2Function(alertBoltNamePrefix, config, numOfAlertBolts), Encoders.tuple(Encoders.STRING(), Encoders.javaSerialization(AlertStreamEvent.class)));
        alertResult.foreachPartition(new AlertPublisherBoltSpark2Function(config, alertPublishBoltName));
        /* StreamingQuery query = alertResult.select("value","_2")..writeStream()
               .outputMode("append")
                .format("console")
                .start();

        query.explain();
        query.awaitTermination();*/
                ;//.foreachPartition(new AlertPublisherBoltSpark2Function(config, alertPublishBoltName));

             /*  .foreachPartition(new AlertPublisherBoltSpark2Function(config, alertPublishBoltName));
        //.foreachPartition(new AlertPublisherBoltSpark2Function(config, alertPublishBoltName));

       StreamingQuery query = alertResult.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.explain();
        query.awaitTermination();*/
               // .repartition(numOfRouter,new Column("index"))..groupBy(new Column("index")).count();

        //.window(Durations.seconds(windowDurations), Durations.seconds(windowDurations))
        // lines.flatMapToPair(new CorrelationSpoutSparkFunction(numOfRouter, topic, config))
               /* .transformToPair(new ChangePartitionTo(numOfRouter))
                .mapPartitionsToPair(new StreamRouteBoltFunction(config, "streamBolt"))
                .transformToPair(new ChangePartitionTo(numOfAlertBolts)).mapPartitionsToPair(new AlertBoltFunction(alertBoltNamePrefix, config, numOfAlertBolts))
                .repartition(numOfPublishTasks).foreachRDD(new Publisher(config, alertPublishBoltName));*/
    }

}
