/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package org.apache.eagle.alert.engine.runner;

import com.typesafe.config.Config;
import kafka.serializer.StringDecoder;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.coordinator.MetadataType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.router.SpoutSpecListener;
import org.apache.eagle.alert.engine.spark.broadcast.SpoutSpecData;
import org.apache.eagle.alert.engine.spark.broadcast.StreamDefinitionData;
import org.apache.eagle.alert.engine.spark.function.CorrelationSpoutSparkFunction;
import org.apache.eagle.alert.engine.spark.function.FilterNullMessageFunction;
import org.apache.eagle.alert.engine.spark.function.PairDataFunction;
import org.apache.eagle.alert.engine.spark.function.TransFormFunction;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class UnitSparkTopologyRunner implements SpoutSpecListener, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkTopologyRunner.class);

    public final static String WINDOW_SECOND = "topology.window.second";
    public final static int DEFAULT_WINDOW_SECOND = 2;
    public final static String SPARK_EXECUTOR_CORES = "topology.core";
    public final static String SPARK_EXECUTOR_MEMORY = "topology.memory";
    public final static String SPARK_EXECUTOR_INSTANCES = "topology.spark.executor.num"; //no need to set if you open spark.dynamicAllocation.enabled  see https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation
    public final static String LOCAL_MODE = "topology.localMode";
    public final static String ROUTER_TASK_NUM = "topology.numOfRouterBolts";

    private final IMetadataChangeNotifyService metadataChangeNotifyService;
    private String topologyId;
    private final Config config;
    private transient JavaStreamingContext jssc;
    private SpoutSpec spoutSpec = null;
    private Map<String, StreamDefinition> sds = null;


    public UnitSparkTopologyRunner(IMetadataChangeNotifyService metadataChangeNotifyService, Config config) {

        this.topologyId = config.getString("topology.name");
        this.config = config;

        long window = config.hasPath(WINDOW_SECOND) ? config.getLong(WINDOW_SECOND) : DEFAULT_WINDOW_SECOND;
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(topologyId);
        boolean localMode = config.getBoolean(LOCAL_MODE);
        if (localMode) {
            LOG.info("Submitting as local mode");
            sparkConf.setMaster("local[2]");
        }
        String sparkExecutorCores = config.getString(SPARK_EXECUTOR_CORES);
        String sparkExecutorMemory = config.getString(SPARK_EXECUTOR_MEMORY);
        sparkConf.set("spark.executor.cores", sparkExecutorCores);
        sparkConf.set("spark.executor.memory", sparkExecutorMemory);
        this.jssc = new JavaStreamingContext(sparkConf, Durations.seconds(window));
        // sc.setLocalProperty("spark.scheduler.pool", "pool1")
        // sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
        this.metadataChangeNotifyService = metadataChangeNotifyService;
        this.metadataChangeNotifyService.registerListener(this);
        this.metadataChangeNotifyService.init(config, MetadataType.SPOUT);
    }

    public void run() {

        buildTopology(jssc, config);
        jssc.start();
        jssc.awaitTermination();
    }


    private void buildTopology(JavaStreamingContext jssc, Config config) {

      /*  val ssc: StreamingContext = ???
        val kafkaParams: Map[String, String] = Map("group.id" -> "terran", ...)

        val numDStreams = 5
        val topics = Map("zerg.hydra" -> 1)
        val kafkaDStreams = (1 to numDStreams).map { _ =>
            KafkaUtils.createStream(ssc, kafkaParams, topics, ...)
        }*/
        //TODO consider use direct API
      /*  List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<JavaPairDStream<String, String>>(numStreams);
        for (int i = 0; i < numStreams; i++) {
            kafkaStreams.add(KafkaUtils.createStream(jssc, zkQuorum, group, topicmap, StorageLevel.MEMORY_AND_DISK_SER()));
            // do some mapping then use dstream() to transform to dstream for union
        }
        JavaPairDStream<String, String> unifiedStream = jssc.union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()));*/
        Map<String, Integer> topicmap = new HashMap<>();
        topicmap.put("oozie", 1);
        Set<String> topics = new HashSet<String>();
        topics.add("oozie");
        String group = "test";
        String zkQuorum = config.getString("spout.kafkaBrokerZkQuorum");
        //final Broadcast<List<String>> blacklist  = JavaWordBlacklist.getInstance(jssc.sparkContext());

       JavaPairDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicmap, StorageLevel.MEMORY_AND_DISK_SER_2());

       // KafkaUtils.createDirectStream(jssc,String.class,String.class, StringDecoder.class,StringDecoder.class,new HashMap<String, String>(),topics);
        while (spoutSpec == null || sds == null) {
            System.out.println("wait to load spoutSpec or sds");
        }
       /* messages.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
            @Override
            public JavaPairRDD<String, String> call(JavaPairRDD<String, String> v1) throws Exception {
                return v1;
            }
        })*/
                /*.foreach(new Function<JavaPairRDD<String, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> v1) throws Exception {
                v1.p
                return null;
            }
        }).foreachRDD(new VoidFunction2<JavaPairRDD<String, String>, Time>() {
            @Override
            public void call(JavaPairRDD<String, String> v1, Time v2) throws Exception {
                v1.pa
            }
        })*/
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        String topic = "oozie";
        messages.map(new CorrelationSpoutSparkFunction(numOfRouter,topic, spoutSpec,sds))
        .filter(new FilterNullMessageFunction())
                .flatMapToPair(new PairDataFunction())
               // .repartition(2)
                .transformToPair(new TransFormFunction(numOfRouter))
                .foreachRDD(new VoidFunction<JavaPairRDD<Integer, Object>>() {
            @Override
            public void call(JavaPairRDD<Integer, Object> integerObjectJavaPairRDD) throws Exception {
                System.out.println("integerObjectJavaPairRDD.getNumPartitions()"+integerObjectJavaPairRDD.getNumPartitions());
                List<Partition> ps = integerObjectJavaPairRDD.partitions();
                for(Partition p:ps){
                    System.out.println(p);
                }
                integerObjectJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Object>>>() {

                    @Override
                    public void call(Iterator<Tuple2<Integer, Object>> v) throws Exception {
                        int count =0;
                        if(v.hasNext()){
                            count++;
                            Tuple2<Integer, Object> a = v.next();
                            System.out.println(a._1());
                            System.out.println(a._2());

                        }
                        System.out.println("__________________________"+count);
                    }
                });
            }
        });

        // jssc.union()


    }


    @Override
    public void onSpoutSpecChange(SpoutSpec spec, Map<String, StreamDefinition> sds) {
        LOG.info("new metadata is updated " + spec);
        this.spoutSpec = SpoutSpecData.getInstance(jssc.sparkContext(), spec).value();
        this.sds = StreamDefinitionData.getInstance(jssc.sparkContext(), sds).value();
    }

}
