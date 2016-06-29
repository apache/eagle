package org.apache.eagle.alert.engine.topology;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.admin.AdminUtils;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.eagle.alert.engine.spark.function.CorrelationSpoutSparkFunction;
import org.apache.eagle.alert.utils.DateTimeUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.junit.Test;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;

/**
 * For online documentation
 * see
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/utils/TestUtils.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/main/scala/kafka/admin/TopicCommand.scala
 * https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/kafka/admin/TopicCommandTest.scala
 */
public class KafkaProducerTest implements Serializable {

    private int brokerId = 0;
    private String topic = "oozie";

    @Test
    public void producerTest() throws InterruptedException, JsonProcessingException, ParseException {

        // setup Zookeeper
        String zkConnect = TestZKUtils.zookeeperConnect();
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper(zkConnect);
        ZkClient zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        int port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        KafkaServer kafkaServer = TestUtils.createServer(config, mock);


        // create topic
        AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);

        // setup producer
        Properties properties = TestUtils.getProducerConfig("localhost:" + port);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        Producer producer = new Producer(producerConfig);


        // send message
        //String logline = "2016-04-27 15:01:14,526  INFO oozieaudit:520 - IP [192.168.7.199], USER [tangjijun], GROUP [pms], APP [My_Workflow], JOBID [0000000-160427140648764-oozie-oozi-W], " + "OPERATION [start], PARAMETER [0000000-160427140648764-oozie-oozi-W], STATUS [SUCCESS], HTTPCODE [200], ERRORCODE [501], ERRORMESSAGE [no problem]";
        Map<String, Object> map1 = new TreeMap<>();
        map1.put("ip", "192.168.7.199");
        map1.put("jobid", "0000000-160427140648764-oozie-oozi-W");
        map1.put("operation", "start");

        map1.put("timestamp", DateTimeUtil.humanDateToMilliseconds("2016-04-27 15:01:14,526"));


        Map<String, Object> map2 = new TreeMap<>();
        map2.put("ip", "192.168.7.199");
        map2.put("jobid", "0000000-160427140648764-oozie-oozi-W");
        map2.put("operation", "end");

        map2.put("timestamp", DateTimeUtil.humanDateToMilliseconds("2016-04-27 15:01:14,526"));

        System.out.println("-----------------" + zkServer.zookeeper().getClientPort());
        System.out.println(zkConnect);
        //   producer.close();
       // int count = 1;

        while (true) {
           /* String logline;
            if (count % 2 == 0) {
                logline = "a";
            } else {
                logline = "b";
            }*/
            ObjectMapper mapper = new ObjectMapper();
            String msg1 = mapper.writeValueAsString(map1);
            String msg2 = mapper.writeValueAsString(map2);
            KeyedMessage<Integer, byte[]> data1 = new KeyedMessage(topic,"tangjijun".getBytes(StandardCharsets.UTF_8), msg1.getBytes(StandardCharsets.UTF_8));
            KeyedMessage<Integer, byte[]> data2 = new KeyedMessage(topic,"pms".getBytes(StandardCharsets.UTF_8), msg2.getBytes(StandardCharsets.UTF_8));

            List<KeyedMessage> messages = new ArrayList<KeyedMessage>();
            messages.add(data1);
            messages.add(data2);
            producer.send(scala.collection.JavaConversions.asScalaBuffer(messages));
            Thread.sleep(3000);
            System.out.println("-----------------");
          //  count++;
        }


       /* // cleanup
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();*/
    }

    @Test
    public void sparkEnvTest() throws InterruptedException {
        String zkQuorum = "localhost:26349";
        String group = "test";
        String topics = "oozie";
        Map<String, Integer> topicmap = new HashMap<>();
        String[] topicsArr = topics.split(",");
        int n = topicsArr.length;
        for (int i = 0; i < n; i++) {
            topicmap.put(topicsArr[i], 1);
        }
        System.setProperty("hadoop.home.dir", "/tmp");
        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(1000));
        JavaPairDStream<String, String> messages = KafkaUtils.createStream(jsc, zkQuorum, group, topicmap);
      //  messages.map(new CorrelationSpoutSparkFunction()).print();
     //   messages.reduceByKey()
    /*    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {

            @Override
            public Iterable<String> call(Tuple2<String, String> arg0)
                    throws Exception {
                return Lists.newArrayList(arg0._1 + "**test ************" + arg0._2);
            }
        });*/
       // messages.print();
        jsc.start();
        jsc.awaitTermination();
    }

    @Test
    public void test() {
      /*  SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));
        jssc.sparkContext().parallelize(1 to 4, 2);*/
    }

}