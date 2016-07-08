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

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import org.apache.eagle.alert.coordination.model.AlertBoltSpec;
import org.apache.eagle.alert.coordination.model.RouterSpec;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.coordinator.MetadataType;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.router.SpecListener;
import org.apache.eagle.alert.engine.spark.broadcast.AlertBoltSpecData;
import org.apache.eagle.alert.engine.spark.broadcast.RouterSpecData;
import org.apache.eagle.alert.engine.spark.broadcast.SpoutSpecData;
import org.apache.eagle.alert.engine.spark.broadcast.StreamDefinitionData;
import org.apache.eagle.alert.engine.spark.function.AlertBoltFunction;
import org.apache.eagle.alert.engine.spark.function.ChangePartitionTo;
import org.apache.eagle.alert.engine.spark.function.CorrelationSpoutSparkFunction;
import org.apache.eagle.alert.engine.spark.function.StreamRouteBoltFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UnitSparkTopologyRunner implements SpecListener{

    private static final Logger LOG = LoggerFactory.getLogger(UnitSparkTopologyRunner.class);

    public final static String WINDOW_SECOND = "topology.window";
    public final static int DEFAULT_WINDOW_SECOND = 2;
    public final static String SPARK_EXECUTOR_CORES = "topology.core";
    public final static String SPARK_EXECUTOR_MEMORY = "topology.memory";
    private final static String alertBoltNamePrefix = "alertBolt";
    public final static String SPARK_EXECUTOR_INSTANCES = "topology.spark.executor.num"; //no need to set if you open spark.dynamicAllocation.enabled  see https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation
    public final static String LOCAL_MODE = "topology.localMode";
    public final static String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    public final static String ALERT_TASK_NUM = "topology.numOfAlertBolts";

    private final IMetadataChangeNotifyService metadataChangeNotifyService;
    private String topologyId;
    private final Config config;
    private JavaStreamingContext jssc;
    private SpoutSpec spoutSpec = null;
    private RouterSpec routerSpec = null;
    private AlertBoltSpec alertBoltSpec = null;
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
            sparkConf.setMaster("local[3]");
        }
        String sparkExecutorCores = config.getString(SPARK_EXECUTOR_CORES);
        String sparkExecutorMemory = config.getString(SPARK_EXECUTOR_MEMORY);
        sparkConf.set("spark.executor.cores", sparkExecutorCores);
        sparkConf.set("spark.executor.memory", sparkExecutorMemory);
        sparkConf.set("spark.streaming.blockInterval", "10000");//20s

        //sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true"); lean in deep
        //conf.set("spark.streaming.backpressure.enabled", "true")
       // ssc.checkpoint("_checkpoint")
        //https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-streaming-kafka.html
        this.jssc = new JavaStreamingContext(sparkConf, Durations.seconds(window));
        // sc.setLocalProperty("spark.scheduler.pool", "pool1")
        // sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
        this.metadataChangeNotifyService = metadataChangeNotifyService;
        this.metadataChangeNotifyService.registerListener(this);
        this.metadataChangeNotifyService.init(config, MetadataType.ALL);
        // this.metadataChangeNotifyService.registerListener((StreamRouterBoltSpecListener)this);
        //  this.metadataChangeNotifyService.init(config, MetadataType.STREAM_ROUTER_BOLT);
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



        // KafkaUtils.createDirectStream(jssc,String.class,String.class, StringDecoder.class,StringDecoder.class,new HashMap<String, String>(),topics);
        while (spoutSpec == null || sds == null || routerSpec == null || alertBoltSpec == null) {
            System.out.println("wait to load spoutSpec or sds");
        }
        JavaPairDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, group, topicmap, StorageLevel.MEMORY_AND_DISK_SER_2());
        // Update the cumulative count function
       /* Function3<String, java.util.Optional<Integer>, State<Object>, Tuple2<Integer, Object>> mappingFunc =
                new Function3<String, java.util.Optional<Integer>, State<Integer>, Tuple2<Integer, Object>>() {
                    @Override
                    public Tuple2<Integer, Object> call(String word, java.util.Optional<Integer> one,
                                                        State<Integer> state) {
                        int sum = one.orElse(0) + (state.exists() ? state.get() : 0);

                        Tuple2<Integer, Object> output = new Tuple2<>(word, sum);
                        state.update(sum);
                        return output;
                    }
                };*/
        int numOfRouter = config.getInt(ROUTER_TASK_NUM);
        int numOfAlertBolts = config.getInt(ALERT_TASK_NUM);
        String topic = "oozie";

       /* StateSpec<Time,Integer,Object,State<Object>> updateState =StateSpec.function(new Function3<Time, Optional<Integer>, State<Object>, State<Object>>() {

            @Override
            public State<Object> call(Time v1, Optional<Integer> key, State<Object> value) throws Exception {
                System.out.println(v1.toString());
                return value;
            }

        });*/
        final Function3<Integer, Optional<Object>, State<Object>, Tuple2<String, Integer>> mappingFunc =
                new Function3<Integer, Optional<Object>, State<Object>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> call(Integer key, Optional<Object> value, State<Object> state) throws Exception {
                        if (state.exists()) {
                          //  PartitionedEvent pEvent = (PartitionedEvent) state.get();
                            Tuple2<String, Integer> output = new Tuple2<String, Integer>(key+"", (Integer) state.get() + 1);
                            state.update((Integer) state.get() + 1);
                            state.remove();
                            return output;
                            //int sum = pEvent.getStreamId();
                        } else {
                            Tuple2<String, Integer> output = new Tuple2<String, Integer>(key+"", 0);
                            state.update(0);
                            return null;
                        }


                    }

                   /* @Override
                    public JavaPairRDD<Integer, Object> call(Integer word, Optional<Integer> one, State<Object> state) {
                        return null;
                    }*/
                };

                messages.window(Durations.seconds(20),Durations.seconds(20))
                .flatMapToPair(new CorrelationSpoutSparkFunction(numOfRouter, topic, spoutSpec, sds))
                .transformToPair(new ChangePartitionTo(numOfRouter))
                 .mapPartitionsToPair(new StreamRouteBoltFunction(routerSpec,sds,"streamBolt"))
                 .transformToPair(new ChangePartitionTo(numOfAlertBolts)).mapPartitionsToPair(new AlertBoltFunction(alertBoltNamePrefix,alertBoltSpec,sds))

       // new StreamRouteBoltFunction(routerSpec,sds)
      //  new StreamRouteBoltFunction(routerSpec,sds,new SparkStreamRouterBoltOutputCollector("streamBolt"))
                //.flatMapToPair()
        .print();
       /* .transformToPair(new ChangePartitionTo(numOfAlertBolts)).foreachRDD(new VoidFunction<JavaPairRDD<Integer, Object>>() {
                    @Override
                    public void call(JavaPairRDD<Integer, Object> integerObjectJavaPairRDD) throws Exception {

                        System.out.println("integerObjectJavaPairRDD.getNumPartitions()"+integerObjectJavaPairRDD.getNumPartitions());
                        List<Partition> ps = integerObjectJavaPairRDD.partitions();
                        for(Partition p:ps){
                            LOG.info(p.toString());
                        }
                        //integerObjectJavaPairRDD.foreachPartition(new AlertBoltFunction(alertBoltNamePrefix+"0",alertBoltSpec,sds));
                    }
                });*/
             //  .print();
               // .mapWithState(StateSpec.function(mappingFunc).numPartitions(4).partitioner(new StreamRoutePartitioner(numOfRouter))).stateSnapshots()



      /*  val updateState = (batchTime: Time, key: Int, value: Option[String], state: State[Int]) => {
            println(s">>> batchTime = $batchTime")
            println(s">>> key       = $key")
            println(s">>> value     = $value")
            println(s">>> state     = $state")
            val sum = value.getOrElse("").size + state.getOption.getOrElse(0)
            state.update(sum)
            Some((key, value, sum)) // mapped value
        }*/

                // .mapWithState(stateSpecFunc)

                //.transformToPair(new ChangePartitionTo(numOfRouter))
                //.mapWithState()
                //.mapPartitionsToPair(new StreamRouterBoltFunction(config,routerSpec,sds))

       /* SparkConf sparkConf = newSparkConf().setAppName("JavaStatefulNetworkWordCount").setMaster("local[*]");
        JavaStreamingContext ssc = newJavaStreamingContext(sparkConf, Durations.seconds(1));
        ssc.checkpoint("./tmp");
        StateSpec<String, Integer, Integer, Tuple2<String, Integer>> mappingFunc = StateSpec.function((word, one, state) -> {
            if (state.isTimingOut()) {
                System.out.println("Timing out the word: " + word);
                returnnewTuple2<String, Integer> (word, state.get());
            } else {
                int sum = one.or(0) + (state.exists() ? state.get() : 0);
                Tuple2<String, Integer> output = newTuple2 < String, Integer>(word, sum);
                state.update(sum);
                return output;
            }
        }); JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
                ssc.socketTextStream(args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER_2).flatMap(x -> Arrays.asList(SPACE.split(x))).mapToPair(w -> newTuple2 < String, Integer > (w, 1)).
        mapWithState(mappingFunc.timeout(Durations.seconds(5)));

        stateDstream.stateSnapshots().print();*/
        // .filter(new FilterNullMessageFunction())/*
        // .flatMapToPair(new PairDataFunction())
        //  .repartition(2)
        //  .transformToPair(new ChangePartitionTo(numOfRouter))

           /*     .foreachRDD(new VoidFunction<JavaPairRDD<Integer, Object>>() {
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
                        while(v.hasNext()){
                            count++;
                            Tuple2<Integer, Object> a = v.next();
                            System.out.println(a._1()+"----"+a._2());

                        }
                        System.out.println("__count____"+count);
                    }
                });
            }
        });*/

        // jssc.union()


    }

    @Override
    public void onSpecChange(SpoutSpec spec, RouterSpec routerSpec, AlertBoltSpec alertBoltSpec, Map<String, StreamDefinition> sds) {
        this.routerSpec = RouterSpecData.getInstance(jssc.sparkContext(), routerSpec).value();
        this.spoutSpec = SpoutSpecData.getInstance(jssc.sparkContext(), spec).value();
        this.sds = StreamDefinitionData.getInstance(jssc.sparkContext(), sds).value();
        this.alertBoltSpec = AlertBoltSpecData.getInstance(jssc.sparkContext(), alertBoltSpec).value();
    }


   /* @Override
    public void onSpoutSpecChange(SpoutSpec spec, Map<String, StreamDefinition> sds) {
        LOG.info("new SpoutSpec metadata is updated " + spec);
        this.spoutSpec = SpoutSpecData.getInstance(jssc.sparkContext(), spec).value();
        this.sds = StreamDefinitionData.getInstance(jssc.sparkContext(), sds).value();
    }

    @Override
    public void onStreamRouteBoltSpecChange(RouterSpec spec, Map<String, StreamDefinition> sds) {
        //reject sds
        LOG.info("new RouterSpec metadata is updated " + spec);
        this.routerSpec = RouterSpecData.getInstance(jssc.sparkContext(), spec).value();
    }*/


}
