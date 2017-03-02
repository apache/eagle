/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.eagle.hadoop.jmx;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.hadoop.jmx.model.TopologyResult;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.*;

public class HadoopTopologyUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopTopologyUpdater.class);

    private HadoopJmxMonitorConfig config;
    private IEagleServiceClient client;

    public HadoopTopologyUpdater(HadoopJmxMonitorConfig config) {
        this.config = config;
        this.client = new EagleServiceClientImpl(config.config);
    }

    public void update(String serviceName, TopologyResult result) {
        Set<String> availableHostNames = new HashSet<String>();
        List<TaggedLogAPIEntity> entitiesForDeletion = new ArrayList<>();
        List<TaggedLogAPIEntity> entitiesToWrite = new ArrayList<>();

        filterEntitiesToWrite(result, availableHostNames, entitiesToWrite);

        String query = String.format("%s[@site=\"%s\"]{*}", serviceName, this.config.site);
        try {
            GenericServiceAPIResponseEntity<TaggedLogAPIEntity> response = client.search().query(query).pageSize(Integer.MAX_VALUE).send();
            if (response.isSuccess() && response.getObj() != null) {
                for (TaggedLogAPIEntity entity : response.getObj()) {
                    if (!availableHostNames.isEmpty() && !availableHostNames.contains(generatePersistKey(entity))) {
                        entitiesForDeletion.add(entity);
                    }
                }
            }
            deleteEntities(entitiesForDeletion, serviceName);
            writeEntities(entitiesToWrite, result.getMetrics(), serviceName);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void filterEntitiesToWrite(TopologyResult result,
                                       Set<String> availableHostnames,
                                       List<TaggedLogAPIEntity> entitiesToWrite) {
        if (!result.getEntities().isEmpty()) {
            for (TaggedLogAPIEntity entity : result.getEntities()) {
                availableHostnames.add(generatePersistKey(entity));
                entitiesToWrite.add(entity);
            }
        } else {
            LOG.warn("Data is in an inconsistent state");
        }
    }

    private void deleteEntities(List<TaggedLogAPIEntity> entities, String serviceName) {
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

    private String generatePersistKey(TaggedLogAPIEntity entity) {
        return new HashCodeBuilder()
                .append(entity.getTags().get(SITE_TAG))
                .append(entity.getTags().get(HOSTNAME_TAG))
                .append(entity.getTags().get(ROLE_TAG))
                .append(entity.getTags().get(RACK_TAG))
                .build().toString();
    }


}
