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
package org.apache.eagle.alert.engine.runner;

import org.apache.eagle.alert.engine.StreamContext;
import org.apache.eagle.alert.engine.coordinator.IMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.coordinator.StreamNotDefinedException;
import org.apache.eagle.alert.engine.model.PartitionedEvent;
import org.apache.eagle.alert.engine.serialization.PartitionedEventSerializer;
import org.apache.eagle.alert.engine.serialization.SerializationMetadataProvider;
import org.apache.eagle.alert.engine.serialization.Serializers;
import org.apache.eagle.alert.utils.AlertConstants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings( {"rawtypes", "serial"})
public abstract class AbstractStreamBolt extends BaseRichBolt implements SerializationMetadataProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamBolt.class);
    private IMetadataChangeNotifyService changeNotifyService;

    public Config getConfig() {
        return config;
    }

    private Config config;
    private List<String> outputStreamIds;
    protected OutputCollector collector;
    protected Map stormConf;

    private String boltId;
    protected PartitionedEventSerializer serializer;
    protected volatile Map<String, StreamDefinition> sdf = new HashMap<String, StreamDefinition>();
    protected volatile String specVersion = "Not Initialized";
    protected volatile boolean specVersionOutofdate = false;
    protected StreamContext streamContext;

    public AbstractStreamBolt(String boltId, IMetadataChangeNotifyService changeNotifyService, Config config) {
        this.boltId = boltId;
        this.changeNotifyService = changeNotifyService;
        this.config = config;
    }

    public void declareOutputStreams(List<String> outputStreamIds) {
        this.outputStreamIds = outputStreamIds;
    }

    protected List<String> getOutputStreamIds() {
        return this.outputStreamIds;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Preconditions.checkNotNull(this.changeNotifyService, "IMetadataChangeNotifyService is not set yet");
        this.stormConf = stormConf;
        this.collector = collector;
        this.serializer = Serializers.newPartitionedEventSerializer(this);
        internalPrepare(collector, this.changeNotifyService, this.config, context);
        try {
            this.changeNotifyService.activateFetchMetaData();
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
        }
    }


    protected PartitionedEvent deserialize(Object object) throws IOException {
        // byte[] in higher priority
        if (object instanceof byte[]) {
            return serializer.deserialize((byte[]) object);
        } else if (object instanceof PartitionedEvent) {
            return (PartitionedEvent) object;
        } else {
            throw new IllegalStateException(String.format("Unsupported event class '%s', expect byte array or PartitionedEvent!", object == null ? null : object.getClass().getCanonicalName()));
        }
    }

    /**
     * subclass should implement more initialization for example.
     * 1) register metadata change
     * 2) init stream context
     *
     * @param collector
     * @param metadataManager
     * @param config
     * @param context
     */
    public abstract void internalPrepare(
        OutputCollector collector,
        IMetadataChangeNotifyService metadataManager,
        Config config, TopologyContext context);

    @Override
    public void cleanup() {
        super.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.outputStreamIds != null) {
            LOG.info("declare streams: {} ", outputStreamIds);
            for (String streamId : this.outputStreamIds) {
                declarer.declareStream(streamId, new Fields(AlertConstants.FIELD_0));
            }
        } else {
            declarer.declare(new Fields(AlertConstants.FIELD_0));
        }
    }

    @Override
    public StreamDefinition getStreamDefinition(String streamId) throws StreamNotDefinedException {
        if (sdf.containsKey(streamId)) {
            return sdf.get(streamId);
        } else {
            throw new StreamNotDefinedException(streamId, specVersion);
        }
    }

    public String getBoltId() {
        return boltId;
    }
}