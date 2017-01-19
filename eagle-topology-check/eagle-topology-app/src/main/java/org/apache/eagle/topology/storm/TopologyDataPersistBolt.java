/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.topology.storm;

import static org.apache.eagle.topology.TopologyConstants.HOSTNAME_TAG;
import static org.apache.eagle.topology.TopologyConstants.ROLE_TAG;
import static org.apache.eagle.topology.TopologyConstants.SITE_TAG;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.apache.eagle.topology.TopologyCheckAppConfig;
import org.apache.eagle.topology.TopologyConstants;
import org.apache.eagle.topology.extractor.TopologyEntityParserResult;
import org.apache.eagle.topology.entity.HBaseServiceTopologyAPIEntity;
import org.apache.eagle.topology.entity.HdfsServiceTopologyAPIEntity;
import org.apache.eagle.topology.entity.HealthCheckParseAPIEntity;
import org.apache.eagle.topology.entity.MRServiceTopologyAPIEntity;
import org.apache.eagle.topology.entity.TopologyBaseAPIEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TopologyDataPersistBolt extends BaseRichBolt {

    private TopologyCheckAppConfig config;
    private IEagleServiceClient client;
    private OutputCollector collector;

    private static final Logger LOG = LoggerFactory.getLogger(TopologyDataPersistBolt.class);

    public TopologyDataPersistBolt(TopologyCheckAppConfig config) {
        this.config = config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.client = new EagleServiceClientImpl(config.getConfig());
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input == null) {
            return;
        }
        String serviceName = input.getStringByField(TopologyConstants.SERVICE_NAME_FIELD);
        TopologyEntityParserResult result = (TopologyEntityParserResult) input.getValueByField(TopologyConstants.TOPOLOGY_DATA_FIELD);
        Set<String> availableHostNames = new HashSet<String>();
        List<TopologyBaseAPIEntity> entitiesForDeletion = new ArrayList<>();
        List<TopologyBaseAPIEntity> entitiesToWrite = new ArrayList<>();

        filterEntitiesToWrite(result, availableHostNames, entitiesToWrite);

        String query = String.format("%s[@site=\"%s\"]{*}", serviceName, this.config.dataExtractorConfig.site);
        try {
            GenericServiceAPIResponseEntity<TopologyBaseAPIEntity> response = client.search().query(query).pageSize(Integer.MAX_VALUE).send();
            if (response.isSuccess() && response.getObj() != null) {
                for (TopologyBaseAPIEntity entity : response.getObj()) {
                    if (!availableHostNames.isEmpty() && !availableHostNames.contains(generatePersistKey(entity))) {
                        entitiesForDeletion.add(entity);
                    }
                }
            }
            deleteEntities(entitiesForDeletion, serviceName);
            writeEntities(entitiesToWrite, result.getMetrics(), serviceName);
            emitToKafkaBolt(result);
            this.collector.ack(input);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            this.collector.fail(input);
        }
    }

    private void filterEntitiesToWrite(TopologyEntityParserResult result, Set<String> availableHostnames, List<TopologyBaseAPIEntity> entitiesToWrite) {
        if (!result.getSlaveNodes().isEmpty()) {
            for (TopologyBaseAPIEntity entity : result.getMasterNodes()) {
                availableHostnames.add(generatePersistKey(entity));
                entitiesToWrite.add(entity);
            }
            for (TopologyBaseAPIEntity entity : result.getSlaveNodes()) {
                availableHostnames.add(generatePersistKey(entity));
                entitiesToWrite.add(entity);
            }
        } else {
            LOG.warn("Data is in an inconsistent state");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("f1"));
    }

    private void deleteEntities(List<TopologyBaseAPIEntity> entities, String serviceName) {
        try {
            GenericServiceAPIResponseEntity response = client.delete(entities);
            if (!response.isSuccess()) {
                LOG.error("Got exception from eagle service: " + response.getException());
            } else {
                LOG.info("Successfully delete {} entities for {}", entities.size(), serviceName);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        entities.clear();
    }

    private void writeEntities(List<? extends TaggedLogAPIEntity> entities, List<GenericMetricEntity> metrics, String serviceName) {
        try {
            GenericServiceAPIResponseEntity response = client.create(entities);
            if (!response.isSuccess()) {
                LOG.error("Got exception from eagle service: " + response.getException());
            } else {
                LOG.info("Successfully wrote {} entities for {}", entities.size(), serviceName);
            }
            response = client.create(metrics);
            if (!response.isSuccess()) {
                LOG.error("Got exception from eagle service: " + response.getException());
            } else {
                LOG.info("Successfully wrote {} metrics for {}", metrics.size(), serviceName);
            }
        } catch (Exception e) {
            LOG.error("cannot create entities successfully", e);
        }
        entities.clear();
    }

    private String generatePersistKey(TopologyBaseAPIEntity entity) {
        return new HashCodeBuilder().append(entity.getTags().get(TopologyConstants.SITE_TAG))
                .append(entity.getTags().get(TopologyConstants.HOSTNAME_TAG))
                .append(entity.getTags().get(TopologyConstants.ROLE_TAG))
                .append(entity.getTags().get(TopologyConstants.RACK_TAG))
                .build().toString();
    }

    private void emitToKafkaBolt(TopologyEntityParserResult result) {
        List<HealthCheckParseAPIEntity> healthCheckParseAPIList = new ArrayList<HealthCheckParseAPIEntity>();
        setNodeInfo(result.getMasterNodes(), healthCheckParseAPIList);
        setNodeInfo(result.getSlaveNodes(), healthCheckParseAPIList);
        for (HealthCheckParseAPIEntity healthCheckAPIEntity : healthCheckParseAPIList) {
            this.collector.emit(new Values(healthCheckAPIEntity));
        }
    }

    private void setNodeInfo(List<TopologyBaseAPIEntity> topologyBaseAPIList, List<HealthCheckParseAPIEntity> healthCheckParseAPIList) {
        HealthCheckParseAPIEntity healthCheckAPIEntity = null;
        for (Iterator<TopologyBaseAPIEntity> iterator = topologyBaseAPIList.iterator(); iterator.hasNext(); ) {

            healthCheckAPIEntity = new HealthCheckParseAPIEntity();
            TopologyBaseAPIEntity topologyBaseAPIEntity = iterator.next();

            if (topologyBaseAPIEntity instanceof HBaseServiceTopologyAPIEntity) {
                healthCheckAPIEntity.setStatus(((HBaseServiceTopologyAPIEntity) topologyBaseAPIEntity).getStatus());
            }
            if (topologyBaseAPIEntity instanceof HdfsServiceTopologyAPIEntity) {
                healthCheckAPIEntity.setStatus(((HdfsServiceTopologyAPIEntity) topologyBaseAPIEntity).getStatus());
            }
            if (topologyBaseAPIEntity instanceof MRServiceTopologyAPIEntity) {
                healthCheckAPIEntity.setStatus(((MRServiceTopologyAPIEntity) topologyBaseAPIEntity).getStatus());
            }

            healthCheckAPIEntity.setTimeStamp(topologyBaseAPIEntity.getTimestamp());
            healthCheckAPIEntity.setHost(topologyBaseAPIEntity.getTags().get(HOSTNAME_TAG));
            healthCheckAPIEntity.setRole(topologyBaseAPIEntity.getTags().get(ROLE_TAG));
            healthCheckAPIEntity.setSite(topologyBaseAPIEntity.getTags().get(SITE_TAG));
            healthCheckParseAPIList.add(healthCheckAPIEntity);
        }
    }
}
