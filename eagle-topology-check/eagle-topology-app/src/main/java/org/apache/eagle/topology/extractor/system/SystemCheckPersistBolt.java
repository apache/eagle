/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.topology.extractor.system;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.BatchSender;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.entity.SystemServiceTopologyAPIEntity;
import org.apache.eagle.topology.resolver.TopologyRackResolver;
import org.apache.eagle.topology.resolver.impl.DefaultTopologyRackResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.eagle.topology.TopologyConstants.*;

/**
 * system check persist bolt.
 */
public class SystemCheckPersistBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(SystemCheckPersistBolt.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final int batchSize;
    private OutputCollector collector;
    private TopologyCheckAppConfig config;
    private IEagleServiceClient client;
    private BatchSender batchSender;
    private TopologyRackResolver rackResolver;
    private String site;

    public SystemCheckPersistBolt(TopologyCheckAppConfig config) {
        this.config = config;
        this.site = config.dataExtractorConfig.site;
        this.batchSize = config.systemConfig.systemInstanceSendBatchSize > 0 ? config.systemConfig.systemInstanceSendBatchSize : 1;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.client = new EagleServiceClientImpl(config.getConfig());
        if (this.batchSize > 0) {
            this.batchSender = client.batch(this.batchSize);
        }
        this.rackResolver = new DefaultTopologyRackResolver();
        if (config.dataExtractorConfig.resolverCls != null) {
            try {
                rackResolver = config.dataExtractorConfig.resolverCls.newInstance();
                rackResolver.prepare(config);
            } catch (InstantiationException | IllegalAccessException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TopologyConstants.SERVICE_NAME_FIELD, TopologyConstants.TOPOLOGY_DATA_FIELD));
    }

    @Override
    public void execute(Tuple tuple) {
        long excuteTimestamp = System.currentTimeMillis();
        if (tuple == null) {
            return;
        }
        // transform tuple to SystemServiceTopologyAPIEntity
        String metrix = tuple.getString(0);
        try {
            SystemServiceTopologyAPIEntity entity = createEntity(metrix, excuteTimestamp);
            if (batchSize <= 1) {
                GenericServiceAPIResponseEntity<String> response = this.client.create(Collections.singletonList(entity));
                if (!response.isSuccess()) {
                    LOG.error("Service side error: {}", response.getException());
                    collector.reportError(new IllegalStateException(response.getException()));
                }
            } else {
                batchSender.send(entity);
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            this.collector.fail(tuple);
        }

    }

    private SystemServiceTopologyAPIEntity createEntity(String metrix, long excuteTimestamp) throws IOException {
        SystemServiceTopologyAPIEntity entity = mapper.readValue(metrix, SystemServiceTopologyAPIEntity.class);
        Map<String, String> tags = new HashMap<String, String>();
        entity.setTags(tags);
        tags.put(SITE_TAG, this.site);
        tags.put(ROLE_TAG, TopologyConstants.SYSTEM_ROLE);
        tags.put(HOSTNAME_TAG, entity.getHost());
        String rack = rackResolver.resolve(entity.getHost());
        tags.put(RACK_TAG, rack);
        entity.setLastUpdateTime(excuteTimestamp);
        entity.setTimestamp(excuteTimestamp);
        // status
        entity.setStatus("active");
        return entity;
    }
}
