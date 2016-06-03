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

package org.apache.eagle.alert.engine.topology;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.eagle.alert.coordination.model.Kafka2TupleMetadata;
import org.apache.eagle.alert.coordination.model.SpoutSpec;
import org.apache.eagle.alert.coordination.model.StreamRepartitionMetadata;
import org.apache.eagle.alert.coordination.model.Tuple2StreamMetadata;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.spout.CorrelationSpout;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;

import storm.kafka.KafkaSpoutWrapper;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

@Ignore
public class CorrelationSpoutTest {
    @SuppressWarnings("unused")
    private static final String TEST_SPOUT_ID = "spout-id-1";
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(CorrelationSpoutTest.class);
    private String dataSourceName = "ds-name";

    @SuppressWarnings({ "serial", "rawtypes" })
    @Test
    public void testMetadataInjestion_emptyMetadata() throws Exception{
        String topoId = "testMetadataInjection";
        Config config = ConfigFactory.load();
        AtomicBoolean validated = new AtomicBoolean(false);
        CorrelationSpout spout = new CorrelationSpout(config, topoId, null, 1) {
            @Override
            protected KafkaSpoutWrapper createKafkaSpout(Map conf, TopologyContext context,
                    SpoutOutputCollector collector, String topic, String schemeClsName, SpoutSpec streamMetadatas, Map<String, StreamDefinition> sds)
                    throws Exception {
                validated.set(true);
                return null;
            }
        };
        Kafka2TupleMetadata ds = new Kafka2TupleMetadata();
        ds.setName("ds-name");
        ds.setType("KAFKA");
        ds.setProperties(new HashMap<String, String>());
        ds.setTopic("name-of-topic1");
        ds.setSchemeCls("PlainStringScheme");
        ds.setCodec(new Tuple2StreamMetadata());
        Map<String,Kafka2TupleMetadata> dsMap = new HashMap<String, Kafka2TupleMetadata>();
        dsMap.put(ds.getName(), ds);

        StreamRepartitionMetadata m1 = new StreamRepartitionMetadata(ds.getName(), "s1");

        Map<String, List<StreamRepartitionMetadata>> dataSources = new HashMap<String, List<StreamRepartitionMetadata>>();
        dataSources.put(ds.getName(), Arrays.asList(m1));

        SpoutSpec newMetadata = new SpoutSpec(topoId, dataSources, null, dsMap);
        
        spout.onReload(newMetadata, null);
        Assert.assertTrue(validated.get());
    }

    @SuppressWarnings({ "serial", "rawtypes" })
    @Test
    public void testMetadataInjestion_oneNewTopic2Streams() throws Exception {
        String topoId = "testMetadataInjection";
        final String topicName = "testTopic";

        Config config = ConfigFactory.load();
        final AtomicBoolean verified = new AtomicBoolean(false);
        CorrelationSpout spout = new CorrelationSpout(config, topoId, null, 1)  {
            @Override
            protected KafkaSpoutWrapper createKafkaSpout(Map conf, 
                    TopologyContext context,
                    SpoutOutputCollector collector, 
                    String topic, 
                    String topic2SchemeClsName,
                    SpoutSpec streamMetadatas,
                    Map<String, StreamDefinition> sds) {
                Assert.assertEquals(1, streamMetadatas.getStreamRepartitionMetadataMap().size());
                Assert.assertTrue(streamMetadatas.getStream("s1") != null);
                Assert.assertTrue(streamMetadatas.getStream("s2") != null);
                Assert.assertEquals(dataSourceName, streamMetadatas.getStream("s1").getTopicName());
                Assert.assertEquals(dataSourceName, streamMetadatas.getStream("s2").getTopicName());
                LOG.info("successfully verified new topic and streams");
                verified.set(true);
                return null;
            }
        };

        Map<String, Kafka2TupleMetadata> dsMap = createDatasource(topicName, dataSourceName);

        StreamRepartitionMetadata m1 = new StreamRepartitionMetadata(dataSourceName, "s1");
        StreamRepartitionMetadata m2 = new StreamRepartitionMetadata(dataSourceName, "s2");
        Map<String, List<StreamRepartitionMetadata>> dataSources = new HashMap<String, List<StreamRepartitionMetadata>>();
        dataSources.put(dataSourceName, Arrays.asList(m1, m2));

        SpoutSpec newMetadata = new SpoutSpec(topoId, dataSources, null, dsMap);
        spout.onReload(newMetadata, null);
        Assert.assertTrue(verified.get());
    }

    private Map<String, Kafka2TupleMetadata> createDatasource(final String topicName, final String dataSourceName) {
        Kafka2TupleMetadata ds = new Kafka2TupleMetadata();
        
        ds.setName(dataSourceName);
        ds.setType("KAFKA");
        ds.setProperties(new HashMap<String, String>());
        ds.setTopic(topicName);
        ds.setSchemeCls("PlainStringScheme");
        ds.setCodec(new Tuple2StreamMetadata());
        Map<String, Kafka2TupleMetadata> dsMap = new HashMap<String, Kafka2TupleMetadata>();
        dsMap.put(ds.getName(), ds);
        return dsMap;
    }

    @SuppressWarnings({ "serial", "rawtypes" })
    @Test
    public void testMetadataInjestion_deleteOneTopic() throws Exception{
        String topoId = "testMetadataInjection";
        final String topicName = "testTopic";
        Config config = ConfigFactory.load();
        final AtomicBoolean verified = new AtomicBoolean(false);
        CorrelationSpout spout = new CorrelationSpout(config, topoId, null, 1) {
            @Override
            protected KafkaSpoutWrapper createKafkaSpout(Map conf, TopologyContext context, SpoutOutputCollector collector, final String topic,
                                                         String schemeClsName, SpoutSpec streamMetadatas,
                                                         Map<String, StreamDefinition> sds){
                return new KafkaSpoutWrapper(null, null);
            }
            @Override
            protected void removeKafkaSpout(KafkaSpoutWrapper wrapper){
                LOG.info("successfully verified removed topic and streams");
                verified.set(true);
            }
        };

        Map<String, Kafka2TupleMetadata> dsMap = createDatasource(topicName, dataSourceName);

        StreamRepartitionMetadata m1 = new StreamRepartitionMetadata(dataSourceName, "s1");

        Map<String, List<StreamRepartitionMetadata>> streamMetadatas = new HashMap<String, List<StreamRepartitionMetadata>>();
        streamMetadatas.put(dataSourceName, Arrays.asList(m1));

        SpoutSpec cachedMetadata = new SpoutSpec(topoId, streamMetadatas, null, dsMap);
        // add new topic
        spout.onReload(cachedMetadata, null);
        // delete new topic
        try {
            spout.onReload(new SpoutSpec(topoId, new HashMap<String, List<StreamRepartitionMetadata>>(), new HashMap<>(), new HashMap<String, Kafka2TupleMetadata>()),
                    null);
        }catch(Exception ex){
            LOG.error("error reloading spout metadata", ex);
            throw ex;
        }
        Assert.assertTrue(verified.get());
    }
}