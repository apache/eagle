/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.metric;

import com.codahale.metrics.*;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kafka.admin.AdminUtils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.apache.eagle.alert.metric.entity.MetricEvent;
import org.apache.eagle.alert.metric.sink.ConsoleSink;
import org.apache.eagle.alert.metric.sink.Slf4jSink;
import org.apache.eagle.alert.metric.source.JVMMetricSource;
import org.apache.eagle.alert.metric.source.MetricSource;
import org.apache.eagle.alert.utils.JsonUtils;
import org.apache.eagle.alert.utils.KafkaEmbedded;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class MetricSystemTest {

    public static final String END_LINE = System.getProperty("line.separator");
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    private static final int DATA_BEGIN_INDEX = 55;
    private static final String TOPIC = "alert_metric_test";
    private static final String clientName = "test";

    @Test
    public void testMetricEvent() {
        MetricEvent metricEvent = MetricEvent.of("test").build();
        Assert.assertEquals(metricEvent.get("name"), "test");
        Assert.assertNotNull(metricEvent.get("timestamp"));

        metricEvent = MetricEvent.of("test1").build();
        metricEvent.put("timestamp", 1);
        Assert.assertEquals(metricEvent.get("name"), "test1");
        Assert.assertEquals(metricEvent.get("timestamp"), 1);

        Counter counter = new Counter();
        counter.inc(10);
        metricEvent = MetricEvent.of("testcount").from(counter).build();
        Assert.assertEquals(metricEvent.get("count"), 10l);

        Gauge gauge = Mockito.mock(FileDescriptorRatioGauge.class);
        Mockito.when(gauge.getValue()).thenReturn(new Double("0.4"));
        metricEvent = MetricEvent.of("testGauge").from(gauge).build();
        Assert.assertEquals(metricEvent.get("value"), 0.4);

        //Histogram
        Histogram histogram = Mockito.mock(Histogram.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(histogram.getCount()).thenReturn(11l);
        Mockito.when(histogram.getSnapshot()).thenReturn(snapshot);
        Mockito.when(snapshot.getMin()).thenReturn(1l);
        Mockito.when(snapshot.getMax()).thenReturn(2l);
        Mockito.when(snapshot.getMean()).thenReturn(3d);
        Mockito.when(snapshot.getStdDev()).thenReturn(4d);
        Mockito.when(snapshot.getMedian()).thenReturn(5d);
        Mockito.when(snapshot.get75thPercentile()).thenReturn(6d);
        Mockito.when(snapshot.get95thPercentile()).thenReturn(7d);
        Mockito.when(snapshot.get98thPercentile()).thenReturn(8d);
        Mockito.when(snapshot.get99thPercentile()).thenReturn(9d);
        Mockito.when(snapshot.get999thPercentile()).thenReturn(10d);
        metricEvent = MetricEvent.of("testHistogram").from(histogram).build();

        Assert.assertEquals(metricEvent.get("count"), 11l);
        Assert.assertEquals(metricEvent.get("min"), 1l);
        Assert.assertEquals(metricEvent.get("max"), 2l);
        Assert.assertEquals(metricEvent.get("mean"), 3d);
        Assert.assertEquals(metricEvent.get("stddev"), 4d);
        Assert.assertEquals(metricEvent.get("median"), 5d);
        Assert.assertEquals(metricEvent.get("75thPercentile"), 6d);
        Assert.assertEquals(metricEvent.get("95thPercentile"), 7d);
        Assert.assertEquals(metricEvent.get("98thPercentile"), 8d);
        Assert.assertEquals(metricEvent.get("99thPercentile"), 9d);
        Assert.assertEquals(metricEvent.get("999thPercentile"), 10d);

        //Meter
        Meter meter = Mockito.mock(Meter.class);
        Mockito.when(meter.getCount()).thenReturn(1l);
        Mockito.when(meter.getOneMinuteRate()).thenReturn(2d);
        Mockito.when(meter.getFiveMinuteRate()).thenReturn(3d);
        Mockito.when(meter.getFifteenMinuteRate()).thenReturn(4d);
        Mockito.when(meter.getMeanRate()).thenReturn(5d);
        metricEvent = MetricEvent.of("testMeter").from(meter).build();

        Assert.assertEquals(metricEvent.get("value"), 1l);
        Assert.assertEquals(metricEvent.get("1MinRate"), 2d);
        Assert.assertEquals(metricEvent.get("5MinRate"), 3d);
        Assert.assertEquals(metricEvent.get("15MinRate"), 4d);
        Assert.assertEquals(metricEvent.get("mean"), 5d);

        //Timer
        Timer value = Mockito.mock(Timer.class);
        Mockito.when(value.getCount()).thenReturn(1l);
        Mockito.when(value.getOneMinuteRate()).thenReturn(2d);
        Mockito.when(value.getFiveMinuteRate()).thenReturn(3d);
        Mockito.when(value.getFifteenMinuteRate()).thenReturn(4d);
        Mockito.when(value.getMeanRate()).thenReturn(5d);
        metricEvent = MetricEvent.of("testTimer").from(value).build();

        Assert.assertEquals(metricEvent.get("value"), 1l);
        Assert.assertEquals(metricEvent.get("1MinRate"), 2d);
        Assert.assertEquals(metricEvent.get("5MinRate"), 3d);
        Assert.assertEquals(metricEvent.get("15MinRate"), 4d);
        Assert.assertEquals(metricEvent.get("mean"), 5d);

    }

    @Test
    public void testMerticSystemWithKafkaSink() throws IOException {

        JVMMetricSource jvmMetricSource = mockMetricRegistry();
        //setup kafka
        KafkaEmbedded kafkaEmbedded = new KafkaEmbedded();
        makeSureTopic(kafkaEmbedded.getZkConnectionString());
        //setup metric system
        File file = genKafkaSinkConfig(kafkaEmbedded.getBrokerConnectionString());
        Config config = ConfigFactory.parseFile(file);
        MetricSystem system = MetricSystem.load(config);
        system.register(jvmMetricSource);
        system.start();
        system.report();

        SimpleConsumer consumer = assertMsgFromKafka(kafkaEmbedded);
        system.stop();
        consumer.close();
        kafkaEmbedded.shutdown();
    }

    @Test
    public void testConsoleSink() throws IOException {
        PrintStream console = System.out;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bytes));

        ConsoleSink sink = new ConsoleSink();
        MetricRegistry registry = new MetricRegistry();
        JvmAttributeGaugeSet jvm = Mockito.mock(JvmAttributeGaugeSet.class);
        Map<String, Metric> metrics = new HashMap<>();
        metrics.put("name", (Gauge) () -> "testname");
        metrics.put("uptime", (Gauge) () -> "testuptime");
        metrics.put("vendor", (Gauge) () -> "testvendor");
        Mockito.when(jvm.getMetrics()).thenReturn(metrics);
        registry.registerAll(jvm);
        File file = genConsoleSinkConfig();
        Config config = ConfigFactory.parseFile(file);
        sink.prepare(config, registry);
        sink.report();
        sink.stop();
        String result = bytes.toString();
        result = result.substring(result.indexOf(END_LINE) + END_LINE.length());//remove first line
        Assert.assertEquals("" + END_LINE + "" +
                "-- Gauges ----------------------------------------------------------------------" + END_LINE + "" +
                "name" + END_LINE + "" +
                "             value = testname" + END_LINE + "" +
                "uptime" + END_LINE + "" +
                "             value = testuptime" + END_LINE + "" +
                "vendor" + END_LINE + "" +
                "             value = testvendor" + END_LINE + "" +
                "" + END_LINE + "" +
                "" + END_LINE + "", result);
        System.setOut(console);
    }

    @Test
    public void testSlf4jSink() throws IOException {
        PrintStream console = System.out;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bytes));

        Slf4jSink sink = new Slf4jSink();
        MetricRegistry registry = new MetricRegistry();
        JvmAttributeGaugeSet jvm = Mockito.mock(JvmAttributeGaugeSet.class);
        Map<String, Metric> metrics = new HashMap<>();
        metrics.put("name", (Gauge) () -> "testname");
        metrics.put("uptime", (Gauge) () -> "testuptime");
        metrics.put("vendor", (Gauge) () -> "testvendor");
        Mockito.when(jvm.getMetrics()).thenReturn(metrics);
        registry.registerAll(jvm);
        File file = genSlf4jSinkConfig();
        Config config = ConfigFactory.parseFile(file);
        sink.prepare(config, registry);
        sink.report();
        sink.stop();
        String result = bytes.toString();
        String finalResult = "";
        Scanner scanner = new Scanner(result);
        while (scanner.hasNext()) {
            finalResult += scanner.nextLine().substring(DATA_BEGIN_INDEX) + END_LINE;
        }
        Assert.assertEquals("type=GAUGE, name=name, value=testname" + END_LINE + "" +
                "type=GAUGE, name=uptime, value=testuptime" + END_LINE + "" +
                "type=GAUGE, name=vendor, value=testvendor" + END_LINE + "", finalResult);
        System.setOut(console);
    }

    private SimpleConsumer assertMsgFromKafka(KafkaEmbedded kafkaEmbedded) throws IOException {
        SimpleConsumer consumer = new SimpleConsumer("localhost", kafkaEmbedded.getPort(), 100000, 64 * 1024, clientName);
        long readOffset = getLastOffset(consumer, TOPIC, 0, kafka.api.OffsetRequest.EarliestTime(), clientName);
        FetchRequest req = new FetchRequestBuilder().clientId(clientName).addFetch(TOPIC, 0, readOffset, 100000).build();
        FetchResponse fetchResponse = consumer.fetch(req);
        Map<Integer, Map<String, String>> resultCollector = new HashMap<>();
        int count = 1;
        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(TOPIC, 0)) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < readOffset) {
                System.out.println("found an old offset: " + currentOffset + " expecting: " + readOffset);
                continue;
            }

            readOffset = messageAndOffset.nextOffset();
            ByteBuffer payload = messageAndOffset.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            String message = new String(bytes, "UTF-8");
            Map<String, String> covertedMsg = JsonUtils.mapper.readValue(message, Map.class);
            covertedMsg.remove("timestamp");
            resultCollector.put(count, covertedMsg);
            count++;
        }
        Assert.assertEquals("{1={name=heap.committed, value=175636480}, 2={name=heap.init, value=262144000}, 3={name=heap.max, value=3704094720}, 4={name=heap.usage, value=0.01570181876990446}, 5={name=heap.used, value=58491576}, 6={name=name, value=testname}, 7={name=non-heap.committed, value=36405248}, 8={name=non-heap.init, value=2555904}, 9={name=non-heap.max, value=-1}, 10={name=non-heap.usage, value=-3.5588712E7}, 11={name=non-heap.used, value=35596496}, 12={name=pools.Code-Cache.usage, value=0.020214080810546875}, 13={name=pools.Compressed-Class-Space.usage, value=0.0035556256771087646}, 14={name=pools.Metaspace.usage, value=0.9777212526244751}, 15={name=pools.PS-Eden-Space.usage, value=0.03902325058129612}, 16={name=pools.PS-Old-Gen.usage, value=0.001959359247654333}, 17={name=pools.PS-Survivor-Space.usage, value=0.0}, 18={name=total.committed, value=212107264}, 19={name=total.init, value=264699904}, 20={name=total.max, value=3704094719}, 21={name=total.used, value=94644240}, 22={name=uptime, value=testuptime}, 23={name=vendor, value=testvendor}}", resultCollector.toString());
        return consumer;
    }

    private JVMMetricSource mockMetricRegistry() {
        JvmAttributeGaugeSet jvm = Mockito.mock(JvmAttributeGaugeSet.class);
        Map<String, Metric> metrics = new HashMap<>();
        metrics.put("name", (Gauge) () -> "testname");
        metrics.put("uptime", (Gauge) () -> "testuptime");
        metrics.put("vendor", (Gauge) () -> "testvendor");
        Mockito.when(jvm.getMetrics()).thenReturn(metrics);
        JVMMetricSource jvmMetricSource = new JVMMetricSource();
        Assert.assertEquals("jvm", jvmMetricSource.name());
        MetricRegistry realRegistry = jvmMetricSource.registry();
        Assert.assertTrue(realRegistry.remove("name"));
        Assert.assertTrue(realRegistry.remove("uptime"));
        Assert.assertTrue(realRegistry.remove("vendor"));
        realRegistry.registerAll(jvm);

        MemoryUsageGaugeSet mem = Mockito.mock(MemoryUsageGaugeSet.class);
        Map<String, Metric> memMetrics = new HashMap<>();
        Assert.assertTrue(realRegistry.remove("heap.committed"));
        Assert.assertTrue(realRegistry.remove("heap.init"));
        Assert.assertTrue(realRegistry.remove("heap.max"));
        Assert.assertTrue(realRegistry.remove("heap.usage"));
        Assert.assertTrue(realRegistry.remove("heap.used"));
        Assert.assertTrue(realRegistry.remove("non-heap.committed"));
        Assert.assertTrue(realRegistry.remove("non-heap.init"));
        Assert.assertTrue(realRegistry.remove("non-heap.max"));
        Assert.assertTrue(realRegistry.remove("non-heap.usage"));
        Assert.assertTrue(realRegistry.remove("non-heap.used"));
        Assert.assertTrue(realRegistry.remove("pools.Code-Cache.usage"));
        Assert.assertTrue(realRegistry.remove("pools.Compressed-Class-Space.usage"));
        Assert.assertTrue(realRegistry.remove("pools.Metaspace.usage"));
        Assert.assertTrue(realRegistry.remove("pools.PS-Eden-Space.usage"));
        Assert.assertTrue(realRegistry.remove("pools.PS-Old-Gen.usage"));
        Assert.assertTrue(realRegistry.remove("pools.PS-Survivor-Space.usage"));
        Assert.assertTrue(realRegistry.remove("total.committed"));
        Assert.assertTrue(realRegistry.remove("total.init"));
        Assert.assertTrue(realRegistry.remove("total.max"));
        memMetrics.put("heap.committed", (Gauge) () -> 175636480);
        memMetrics.put("heap.init", (Gauge) () -> 262144000);
        memMetrics.put("heap.max", (Gauge) () -> 3704094720l);
        memMetrics.put("heap.usage", (Gauge) () -> 0.01570181876990446);
        memMetrics.put("heap.used", (Gauge) () -> 58491576);
        memMetrics.put("non-heap.committed", (Gauge) () -> 36405248);
        memMetrics.put("non-heap.init", (Gauge) () -> 2555904);
        memMetrics.put("non-heap.max", (Gauge) () -> -1);
        memMetrics.put("non-heap.usage", (Gauge) () -> -3.5588712E7);
        memMetrics.put("non-heap.used", (Gauge) () -> 35596496);
        memMetrics.put("pools.Code-Cache.usage", (Gauge) () -> 0.020214080810546875);
        memMetrics.put("pools.Compressed-Class-Space.usage", (Gauge) () -> 0.0035556256771087646);
        memMetrics.put("pools.Metaspace.usage", (Gauge) () -> 0.9777212526244751);
        memMetrics.put("pools.PS-Eden-Space.usage", (Gauge) () -> 0.03902325058129612);
        memMetrics.put("pools.PS-Old-Gen.usage", (Gauge) () -> 0.001959359247654333);
        memMetrics.put("pools.PS-Survivor-Space.usage", (Gauge) () -> 0.0);
        memMetrics.put("total.committed", (Gauge) () -> 212107264);
        memMetrics.put("total.init", (Gauge) () -> 264699904);
        memMetrics.put("total.max", (Gauge) () -> 3704094719l);
        memMetrics.put("total.used", (Gauge) () -> 94644240);
        Mockito.when(mem.getMetrics()).thenReturn(memMetrics);
        Assert.assertTrue(realRegistry.remove("total.used"));
        realRegistry.registerAll(mem);
        return jvmMetricSource;
    }

    private long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            System.out.println("error fetching data offset data the broker. reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private File genKafkaSinkConfig(String brokerConnectionString) throws IOException {
        File file = tempFolder.newFile("application.conf");
        String fileContent = "{" + END_LINE + "" +
                "  metric {" + END_LINE + "" +
                "    sink {" + END_LINE + "" +
                "       kafka {" + END_LINE + "" +
                "        \"topic\": \"" + TOPIC + "\"" + END_LINE + "" +
                "        \"bootstrap.servers\": \"" + brokerConnectionString + "\"" + END_LINE + "" +
                "      }" + END_LINE + "" +
                "    }" + END_LINE + "" +
                "  }" + END_LINE + "" +
                "}";
        FileUtils.writeStringToFile(file, fileContent);
        return file;
    }

    private File genConsoleSinkConfig() throws IOException {
        File file = tempFolder.newFile("application-console.conf");
        String fileContent = "{" + END_LINE + "" +
                "  metric {" + END_LINE + "" +
                "    sink {" + END_LINE + "" +
                "      stdout {" + END_LINE + "" +
                "        // console metric sink" + END_LINE + "" +
                "      }" + END_LINE + "" +
                "    }" + END_LINE + "" +
                "  }" + END_LINE + "" +
                "}";
        FileUtils.writeStringToFile(file, fileContent);
        return file;
    }

    private File genSlf4jSinkConfig() throws IOException {
        File file = tempFolder.newFile("application-slf4j.conf");
        String fileContent = "{" + END_LINE + "" +
                "        metric {" + END_LINE + "" +
                "        sink {" + END_LINE + "" +
                "            logger {" + END_LINE + "" +
                "                level = \"INFO\"" + END_LINE + "" +
                "            }" + END_LINE + "" +
                "        }" + END_LINE + "" +
                "    }" + END_LINE + "" +
                "    }";
        FileUtils.writeStringToFile(file, fileContent);
        return file;
    }

    public void makeSureTopic(String zkConnectionString) {
        ZkClient zkClient = new ZkClient(zkConnectionString, 10000, 10000, ZKStringSerializer$.MODULE$);
        Properties topicConfiguration = new Properties();
        AdminUtils.createTopic(zkClient, TOPIC, 1, 1, topicConfiguration);
    }


    @Test
    @Ignore
    public void testMetaConflict() {
        MetricSystem system = MetricSystem.load(ConfigFactory.load());
        system.register(new MetaConflictMetricSource());
        system.start();
        system.report();
        system.stop();
    }

    private class MetaConflictMetricSource implements MetricSource {
        private MetricRegistry registry = new MetricRegistry();

        public MetaConflictMetricSource() {
            registry.register("meta.conflict", (Gauge<String>) () -> "meta conflict happening!");
        }

        @Override
        public String name() {
            return "metaConflict";
        }

        @Override
        public MetricRegistry registry() {
            return registry;
        }
    }

    private class SampleMetricSource implements MetricSource {
        private MetricRegistry registry = new MetricRegistry();

        public SampleMetricSource() {
            registry.register("sample.long", (Gauge<Long>) System::currentTimeMillis);
            registry.register("sample.map", (Gauge<Map<String, Object>>) () -> new HashMap<String, Object>() {
                private static final long serialVersionUID = 3948508906655117683L;

                {
                    put("int", 1234);
                    put("str", "text");
                    put("bool", true);
                }
            });
        }

        @Override
        public String name() {
            return "sampleSource";
        }

        @Override
        public MetricRegistry registry() {
            return registry;
        }
    }
}