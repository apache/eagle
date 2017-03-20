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
package org.apache.eagle.app.messaging;

import backtype.storm.Testing;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.typesafe.config.Config;
import org.apache.eagle.app.environment.builder.MetricDescriptor;
import org.apache.eagle.app.utils.StreamConvertHelper;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.*;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest( {MetricStreamPersist.class})
public class MetricStreamPersistTest {

    @Test
    public void testStructuredMetricMapper() throws Exception {
        MetricDescriptor metricDefinition = MetricDescriptor
            .metricGroupByField("group")
            .siteAs("siteId")
            .namedByField("metric")
            .eventTimeByField("timestamp")
            .dimensionFields("host", "component", "site")
            .granularity(Calendar.MINUTE)
            .valueField("value");
        Config config = mock(Config.class);
        MetricStreamPersist metricStreamPersist = new MetricStreamPersist(metricDefinition, config);
        Field mapperField = metricStreamPersist.getClass().getDeclaredField("mapper");
        mapperField.setAccessible(true);

        Map event = new HashMap();
        event.put("timestamp", 1482106479564L);
        event.put("metric", "hadoop.memory.heapmemoryusage.used");
        event.put("component", "hbasemaster");
        event.put("site", "sandbox");
        event.put("value", 14460904.0);
        event.put("host", "xxx-xxx.int.xxx.com");

        Tuple tuple = Testing.testTuple(new Values("metric", event));
        MetricStreamPersist.MetricMapper mapper = (MetricStreamPersist.MetricMapper) mapperField.get(metricStreamPersist);

        GenericMetricEntity metricEntity = mapper.map(StreamConvertHelper.tupleToEvent(tuple).f1());

        Assert.assertEquals("prefix:hadoop.memory.heapmemoryusage.used, timestamp:1482106440000, humanReadableDate:2016-12-19 00:14:00,000, tags: component=hbasemaster,site=sandbox,host=xxx-xxx.int.xxx.com,, encodedRowkey:null", metricEntity.toString());
    }

    @Test
    public void testMetricStreamPersist() throws Exception {
        List<String> result = new ArrayList<>();
        OutputCollector collector = new OutputCollector(new IOutputCollector() {
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
                result.add(String.valueOf(tuple.get(0)));
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

        MetricDescriptor metricDefinition = MetricDescriptor
            .metricGroupByField("group")
            .siteAs("siteId")
            .namedByField("metric")
            .eventTimeByField("timestamp")
            .dimensionFields("host", "component", "site")
            .granularity(Calendar.MINUTE)
            .valueField("value");
        Config config = mock(Config.class);
        when(config.hasPath("service.batchSize")).thenReturn(false);

        GenericServiceAPIResponseEntity<String> response = mock(GenericServiceAPIResponseEntity.class);
        when(response.isSuccess()).thenReturn(true);

        EagleServiceClientImpl client = mock(EagleServiceClientImpl.class);
        PowerMockito.whenNew(EagleServiceClientImpl.class).withArguments(config).thenReturn(client);
        when(client.create(anyObject())).thenReturn(response);

        MetricStreamPersist metricStreamPersist = new MetricStreamPersist(metricDefinition, config);
        metricStreamPersist.prepare(null, null, collector);
        Map event = new HashMap();
        event.put("timestamp", 1482106479564L);
        event.put("metric", "hadoop.memory.heapmemoryusage.used");
        event.put("component", "hbasemaster");
        event.put("site", "sandbox");
        event.put("value", 14460904.0);
        event.put("host", "xxx-xxx.int.xxx.com");

        Tuple tuple = Testing.testTuple(new Values("metric", event));
        metricStreamPersist.execute(tuple);
        Assert.assertTrue(result.size() == 1);
        Assert.assertEquals("hadoop.memory.heapmemoryusage.used", result.get(0));
    }
}

