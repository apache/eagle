package org.apache.eagle.alert.engine.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.utils.AlertConstants;
import org.apache.eagle.alert.utils.StreamIdConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
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
@SuppressWarnings("serial")
public class MockStreamReceiver extends BaseRichSpout {
    private final static Logger LOG = LoggerFactory.getLogger(MockStreamReceiver.class);
    private SpoutOutputCollector collector;
    private List<String> outputStreamIds;

    public MockStreamReceiver(int partition) {
        outputStreamIds = new ArrayList<>(partition);
        for (int i = 0; i < partition; i++) {
            outputStreamIds.add(StreamIdConversion.generateStreamIdByPartition(i));
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void close() {
    }

    /**
     * This unit test is not to mock the end2end logic of correlation spout,
     * but simply generate some sample data for following bolts testing
     */
    @Override
    public void nextTuple() {
        PartitionedEvent event = MockSampleMetadataFactory.createRandomOutOfTimeOrderEventGroupedByName("sampleStream_1");
        LOG.info("Receive {}", event);
        collector.emit(outputStreamIds.get(
            // group by the first field in event i.e. name
            (int) (event.getPartitionKey() % outputStreamIds.size())),
            Collections.singletonList(event));
        Utils.sleep(500);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (String streamId : outputStreamIds) {
            declarer.declareStream(streamId, new Fields(AlertConstants.FIELD_0));
        }
    }
}