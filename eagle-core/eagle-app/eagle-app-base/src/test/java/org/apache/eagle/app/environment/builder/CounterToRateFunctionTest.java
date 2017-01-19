/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.app.environment.builder;

import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import org.apache.eagle.app.utils.ClockWithOffset;
import org.apache.eagle.app.utils.ManualClock;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class CounterToRateFunctionTest {


    private Map mkCountTypeEvent(long ts, double value) {
        Map event = new HashMap();
        event.put("timestamp", ts);
        event.put("metric", "hadoop.hbase.regionserver.server.totalrequestcount");
        event.put("component", "hbasemaster");
        event.put("site", "sandbox");
        event.put("value", value);
        event.put("host", "xxx-xxx.int.xxx.com");
        return event;
    }

    private Map mkCountTypeEventWithMetricName(long ts, double value, String metric) {
        Map event = new HashMap();
        event.put("timestamp", ts);
        event.put("metric", metric);
        event.put("component", "hbasemaster");
        event.put("site", "sandbox");
        event.put("value", value);
        event.put("host", "xxx-xxx.int.xxx.com");
        return event;
    }

    private Map mkOtherTypeEvent(long ts, double value) {
        Map event = new HashMap();
        event.put("timestamp", ts);
        event.put("metric", "hadoop.memory.heapmemoryusage.used");
        event.put("component", "hbasemaster");
        event.put("site", "sandbox");
        event.put("value", value);
        event.put("host", "xxx-xxx.int.xxx.com");
        return event;
    }


    @Test
    public void testToMetricAndCounterValue() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        long baseTime = System.currentTimeMillis() + 100000L;

        MetricDefinition metricDefinition = MetricDefinition
                .metricType("HADOOP_JMX_METRICS")
                .namedByField("metric")
                .eventTimeByField("timestamp")
                .dimensionFields("host", "component", "site")
                .granularity(Calendar.MINUTE)
                .valueField("value");
        CounterToRateFunction counterToRateFunction = new CounterToRateFunction(metricDefinition, 3, TimeUnit.MINUTES, ClockWithOffset.INSTANCE);

        Map event = mkCountTypeEvent((baseTime + 0), 374042741.0);
        Method toMetricMethod = counterToRateFunction.getClass().getDeclaredMethod("toMetric", Map.class);
        toMetricMethod.setAccessible(true);
        CounterToRateFunction.Metric metric = (CounterToRateFunction.Metric) toMetricMethod.invoke(counterToRateFunction, event);
        Assert.assertEquals("xxx-xxx.int.xxx.com-hbasemaster-sandbox-hadoop.hbase.regionserver.server.totalrequestcount", metric.getMetricName());
        Assert.assertEquals(374042741.0, Double.valueOf(metric.getValue().toString()), 0.00001);
        Assert.assertEquals(374042741.0, metric.getNumberValue().doubleValue(), 0.00001);
        Assert.assertTrue(metric.isCounter());


        event = mkOtherTypeEvent((baseTime + 0), 100);
        metric = (CounterToRateFunction.Metric) toMetricMethod.invoke(counterToRateFunction, event);
        Assert.assertEquals("xxx-xxx.int.xxx.com-hbasemaster-sandbox-hadoop.memory.heapmemoryusage.used", metric.getMetricName());
        Assert.assertEquals(100, Double.valueOf(metric.getValue().toString()), 0.00001);
        Assert.assertEquals(100, metric.getNumberValue().doubleValue(), 0.00001);
        Assert.assertTrue(!metric.isCounter());


    }

    @Test
    public void testTransformToRate() throws NoSuchFieldException, IllegalAccessException {
        List<Map> result = new ArrayList<>();
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                result.add((Map) tuple.get(1));
                return null;
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {

            }

            @Override
            public void ack(Tuple input) {

            }

            @Override
            public void fail(Tuple input) {

            }

            @Override
            public void reportError(Throwable error) {

            }
        });
        MetricDefinition metricDefinition = MetricDefinition
                .metricType("HADOOP_JMX_METRICS")
                .namedByField("metric")
                .eventTimeByField("timestamp")
                .dimensionFields("host", "component", "site")
                .granularity(Calendar.MINUTE)
                .valueField("value");
        CounterToRateFunction counterToRateFunction = new CounterToRateFunction(metricDefinition, 3, TimeUnit.MINUTES, ClockWithOffset.INSTANCE);
        counterToRateFunction.open(new StormOutputCollector(collector));
        long baseTime = System.currentTimeMillis() + 100000L;
        //put first count sample
        Map event = mkCountTypeEvent((baseTime + 0), 374042741.0);
        counterToRateFunction.transform(event);
        Assert.assertTrue(result.isEmpty());

        Field cacheField = counterToRateFunction.getClass().getDeclaredField("cache");
        cacheField.setAccessible(true);
        Map<String, CounterToRateFunction.CounterValue> cache = (Map<String, CounterToRateFunction.CounterValue>) cacheField.get(counterToRateFunction);
        Assert.assertTrue(cache.size() == 1);

        CounterToRateFunction.CounterValue counterValue = cache.get("xxx-xxx.int.xxx.com-hbasemaster-sandbox-hadoop.hbase.regionserver.server.totalrequestcount");
        Assert.assertEquals((long) event.get("timestamp"), counterValue.getTimestamp());
        Field valueField = counterValue.getClass().getDeclaredField("value");
        valueField.setAccessible(true);
        double value = (double) valueField.get(counterValue);
        Assert.assertEquals(374042741.0, value, 0.00001);
        result.clear();
        //put not count sample
        event = mkOtherTypeEvent((baseTime + 0), 100);
        counterToRateFunction.transform(event);
        Assert.assertTrue(result.size() == 1);
        Assert.assertTrue(cache.size() == 1);
        Assert.assertEquals(baseTime + 0, counterValue.getTimestamp());
        Assert.assertEquals(374042741.0, value, 0.00001);

        Assert.assertEquals("hadoop.memory.heapmemoryusage.used", event.get("metric"));
        Assert.assertEquals(100, (Double) event.get("value"), 0.00001);
        result.clear();

        //delta of 10 in 5 seconds
        event = mkCountTypeEvent((baseTime + 5000), 374042751.0);
        counterToRateFunction.transform(event);

        Assert.assertTrue(result.size() == 1);
        Map transedEvent = result.get(0);
        Assert.assertEquals(baseTime + 5000, transedEvent.get("timestamp"));
        Assert.assertEquals(2.0, (double) transedEvent.get("value"), 0.00001);
        Assert.assertEquals(baseTime + 5000, counterValue.getTimestamp());
        value = (double) valueField.get(counterValue);
        Assert.assertEquals(374042751.0, value, 0.00001);
        result.clear();

        //delta of 15 in 5 seconds
        event = mkCountTypeEvent((baseTime + 10000), 374042766.0);
        counterToRateFunction.transform(event);

        Assert.assertTrue(result.size() == 1);
        transedEvent = result.get(0);
        Assert.assertEquals(baseTime + 10000, transedEvent.get("timestamp"));
        Assert.assertEquals(3.0, (double) transedEvent.get("value"), 0.00001);
        Assert.assertEquals(baseTime + 10000, counterValue.getTimestamp());
        value = (double) valueField.get(counterValue);
        Assert.assertEquals(374042766.0, value, 0.00001);
        result.clear();


        //No change from previous sample
        event = mkCountTypeEvent((baseTime + 15000), 374042766.0);
        counterToRateFunction.transform(event);

        Assert.assertTrue(result.size() == 1);
        transedEvent = result.get(0);
        Assert.assertEquals(baseTime + 15000, transedEvent.get("timestamp"));
        Assert.assertEquals(0.0, (double) transedEvent.get("value"), 0.00001);
        Assert.assertEquals(baseTime + 15000, counterValue.getTimestamp());
        value = (double) valueField.get(counterValue);
        Assert.assertEquals(374042766.0, value, 0.00001);
        result.clear();

        //Decrease from previous sample
        event = mkCountTypeEvent((baseTime + 20000), 1.0);
        counterToRateFunction.transform(event);

        Assert.assertTrue(result.size() == 1);
        transedEvent = result.get(0);
        Assert.assertEquals(baseTime + 20000, transedEvent.get("timestamp"));
        Assert.assertEquals(0.0, (double) transedEvent.get("value"), 0.00001);
        Assert.assertEquals(baseTime + 20000, counterValue.getTimestamp());
        value = (double) valueField.get(counterValue);
        Assert.assertEquals(1.0, value, 0.00001);
        result.clear();
    }

    @Test
    public void testTransformToRateWithExpiration() throws NoSuchFieldException, IllegalAccessException {

        MetricDefinition metricDefinition = MetricDefinition
                .metricType("HADOOP_JMX_METRICS")
                .namedByField("metric")
                .eventTimeByField("timestamp")
                .dimensionFields("host", "component", "site")
                .granularity(Calendar.MINUTE)
                .valueField("value");
        List<Map> result = new ArrayList<>();
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                result.add((Map) tuple.get(1));
                return null;
            }

            @Override
            public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {

            }

            @Override
            public void ack(Tuple input) {

            }

            @Override
            public void fail(Tuple input) {

            }

            @Override
            public void reportError(Throwable error) {

            }
        });
        ManualClock manualClock = new ManualClock(0);
        manualClock.set(30000L);
        CounterToRateFunction counterToRateFunction = new CounterToRateFunction(metricDefinition, 60, TimeUnit.SECONDS, manualClock);
        counterToRateFunction.open(new StormOutputCollector(collector));
        Map event = mkCountTypeEventWithMetricName(manualClock.now(), 110, "hadoop.hbase.regionserver.server.totalrequestcount");
        counterToRateFunction.transform(event);
        Field cacheField = counterToRateFunction.getClass().getDeclaredField("cache");
        cacheField.setAccessible(true);
        Map<String, CounterToRateFunction.CounterValue> cache = (Map<String, CounterToRateFunction.CounterValue>) cacheField.get(counterToRateFunction);
        Assert.assertTrue(cache.size() == 1);

        manualClock.set(50000L);
        event = mkCountTypeEventWithMetricName(manualClock.now(), 130, "hadoop.hbase.regionserver.server.readerrequestcount");
        counterToRateFunction.transform(event);

        cache = (Map<String, CounterToRateFunction.CounterValue>) cacheField.get(counterToRateFunction);
        Assert.assertEquals(2, cache.size());
        Assert.assertEquals("CounterValue{timestamp=30000, value=110.0}", cache.get("xxx-xxx.int.xxx.com-hbasemaster-sandbox-hadoop.hbase.regionserver.server.totalrequestcount").toString());
        Assert.assertEquals("CounterValue{timestamp=50000, value=130.0}", cache.get("xxx-xxx.int.xxx.com-hbasemaster-sandbox-hadoop.hbase.regionserver.server.readerrequestcount").toString());

        manualClock.set(100000L);
        event = mkCountTypeEventWithMetricName(manualClock.now(), 120, "hadoop.hbase.regionserver.server.totalrequestcount");
        counterToRateFunction.transform(event);

        cache = (Map<String, CounterToRateFunction.CounterValue>) cacheField.get(counterToRateFunction);
        Assert.assertEquals(2, cache.size());
        Assert.assertEquals("CounterValue{timestamp=100000, value=120.0}", cache.get("xxx-xxx.int.xxx.com-hbasemaster-sandbox-hadoop.hbase.regionserver.server.totalrequestcount").toString());
        Assert.assertEquals("CounterValue{timestamp=50000, value=130.0}", cache.get("xxx-xxx.int.xxx.com-hbasemaster-sandbox-hadoop.hbase.regionserver.server.readerrequestcount").toString());

        manualClock.set(160001L);
        event = mkCountTypeEventWithMetricName(manualClock.now(), 10, "hadoop.hbase.regionserver.server.writerrequestcount");
        counterToRateFunction.transform(event);
        Assert.assertEquals(2, cache.size());
        Assert.assertEquals("CounterValue{timestamp=160001, value=10.0}", cache.get("xxx-xxx.int.xxx.com-hbasemaster-sandbox-hadoop.hbase.regionserver.server.writerrequestcount").toString());
        Assert.assertEquals("CounterValue{timestamp=50000, value=130.0}", cache.get("xxx-xxx.int.xxx.com-hbasemaster-sandbox-hadoop.hbase.regionserver.server.readerrequestcount").toString());


    }
}
