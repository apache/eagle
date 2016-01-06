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

package org.apache.eagle.datastream.storm;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.dao.AlertDefinitionDAOImpl;
import org.apache.eagle.alert.policy.DefaultPolicyPartitioner;
import org.apache.eagle.datastream.JavaStormStreamExecutor;
import org.apache.eagle.executor.AlertExecutor;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * test JavaStormBoltWrapper
 */
public class TestJavaStormBoltWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(TestJavaStormBoltWrapper.class);

    public static void main(String[] args) throws Exception{
        Config config = ConfigFactory.load();
        EagleServiceConnector connector = new EagleServiceConnector(config);
        String streamName = "eventStream";
        AlertExecutor executor = new AlertExecutor("eventStreamExecutor",
                new DefaultPolicyPartitioner(), 1, 0,
                new AlertDefinitionDAOImpl(connector),
                new String[]{streamName});
        executor.prepareConfig(config);
        JavaStormBoltWrapper bolt = new JavaStormBoltWrapper(config, (JavaStormStreamExecutor)executor);
        bolt.prepare(null, null, getOutputCollector());
        TreeMap map = new TreeMap();
        map.put("name", "xyzzzz");
        map.put("value", 100);
        map.put("timestamp", 14786203663L);
        while(true) {
            LOG.info("sending message ...");
            Tuple tuple = new TestTuple("key1", streamName, map);
            bolt.execute(tuple);
            Thread.sleep(1000);
        }
    }

    private static OutputCollector getOutputCollector(){
        return new OutputCollector(new IOutputCollector(){
            @Override
            public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
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
    }

    private static class TestTuple implements Tuple{
        private String key;
        private String streamName;
        private SortedMap map;
        public TestTuple(final String key, final String streamName, final SortedMap map){
            this.key = key;
            this.streamName = streamName;
            this.map = map;
        }
        @Override
        public GlobalStreamId getSourceGlobalStreamid() {
            return null;
        }
        @Override
        public String getSourceComponent() {
            return null;
        }
        @Override
        public int getSourceTask() {
            return 0;
        }
        @Override
        public String getSourceStreamId() {
            return null;
        }
        @Override
        public MessageId getMessageId() {
            return null;
        }
        @Override
        public int size() {
            return 0;
        }
        @Override
        public boolean contains(String field) {
            return false;
        }
        @Override
        public Fields getFields() {
            return null;
        }
        @Override
        public int fieldIndex(String field) {
            return 0;
        }
        @Override
        public List<Object> select(Fields selector) {
            return null;
        }

        @Override
        public Object getValue(int i) {
            return null;
        }

        @Override
        public String getString(int i) {
            return null;
        }

        @Override
        public Integer getInteger(int i) {
            return null;
        }

        @Override
        public Long getLong(int i) {
            return null;
        }

        @Override
        public Boolean getBoolean(int i) {
            return null;
        }

        @Override
        public Short getShort(int i) {
            return null;
        }

        @Override
        public Byte getByte(int i) {
            return null;
        }

        @Override
        public Double getDouble(int i) {
            return null;
        }

        @Override
        public Float getFloat(int i) {
            return null;
        }

        @Override
        public byte[] getBinary(int i) {
            return new byte[0];
        }

        @Override
        public Object getValueByField(String field) {
            return null;
        }

        @Override
        public String getStringByField(String field) {
            return null;
        }

        @Override
        public Integer getIntegerByField(String field) {
            return null;
        }

        @Override
        public Long getLongByField(String field) {
            return null;
        }

        @Override
        public Boolean getBooleanByField(String field) {
            return null;
        }

        @Override
        public Short getShortByField(String field) {
            return null;
        }

        @Override
        public Byte getByteByField(String field) {
            return null;
        }

        @Override
        public Double getDoubleByField(String field) {
            return null;
        }

        @Override
        public Float getFloatByField(String field) {
            return null;
        }

        @Override
        public byte[] getBinaryByField(String field) {
            return new byte[0];
        }

        @Override
        public List<Object> getValues() {
            List<Object> ret = new ArrayList<Object>();
            ret.add(key);
            ret.add(streamName);
            ret.add(map);
            return ret;
        }
    }
}
