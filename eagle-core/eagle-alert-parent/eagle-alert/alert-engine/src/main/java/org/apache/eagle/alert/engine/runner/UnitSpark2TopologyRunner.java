package org.apache.eagle.alert.engine.runner;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.spark.function2.AlertBoltSpark2Function;
import org.apache.eagle.alert.engine.spark.function2.AlertPublisherBoltSpark2Function;
import org.apache.eagle.alert.engine.spark.function2.CorrelationSpoutSpark2Function;
import org.apache.eagle.alert.engine.spark.function2.StreamRouteBoltSpark2Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * nc -lk 9999
 * oozie {"ip":"yyy.yyy.yyy.yyy","jobid":"0000000-160427140648764-oozie-oozi-W","operation":"start","timestamp":1467884414526}
 */
public class UnitSpark2TopologyRunner {

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkTopologyRunner.class);

    private final static String WINDOW_SECOND = "topology.window";
    private final static int DEFAULT_WINDOW_SECOND = 2;
    private final static String SPARK_EXECUTOR_CORES = "topology.core";
    private final static String SPARK_EXECUTOR_MEMORY = "topology.memory";
    private final static String alertBoltNamePrefix = "alertBolt";
    private final static String alertPublishBoltName = "alertPublishBolt";
    private final static String SPARK_EXECUTOR_INSTANCES = "topology.spark.executor.num"; //no need to set if you open spark.dynamicAllocation.enabled  see https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation
    private final static String LOCAL_MODE = "topology.localMode";
    private final static String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    private final static String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    private final static String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";
    private final static String WINDOW_DURATIONS = "topology.windowDurations";


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

        int windowDurations = config.getInt(WINDOW_DURATIONS);
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        int numOfPublishTasks = config.getInt(PUBLISH_TASK_NUM);


        Dataset<String> lines = sparkSession
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load().as(Encoders.STRING());

        Dataset<Tuple2<Integer, PartitionedEvent>> line = lines.flatMap(new CorrelationSpoutSpark2Function(numOfRouter, config), Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(PartitionedEvent.class)));

        Dataset<Tuple2<Integer, PartitionedEvent>> routblotResult = line.repartition(numOfRouter)
                .mapPartitions(new StreamRouteBoltSpark2Function(config, "streamBolt"), Encoders.tuple(Encoders.INT(), Encoders.javaSerialization(PartitionedEvent.class)));

        Dataset<Tuple2<String, String>> alertResult = routblotResult
                .repartition(numOfAlertBolts)
                .mapPartitions(new AlertBoltSpark2Function(alertBoltNamePrefix, config, numOfAlertBolts), Encoders.tuple(Encoders.STRING(), Encoders.javaSerialization(AlertStreamEvent.class)))
                .repartition(numOfPublishTasks)
                .mapPartitions(new AlertPublisherBoltSpark2Function(config, alertPublishBoltName), Encoders.tuple(Encoders.STRING(), Encoders.STRING()));

        StreamingQuery query = alertResult.toDF("flag", "policy").groupBy("flag", "policy").count().writeStream()
                .outputMode("complete")
                .format("console")
                .start();
        query.explain();
        query.awaitTermination();
    }

}
