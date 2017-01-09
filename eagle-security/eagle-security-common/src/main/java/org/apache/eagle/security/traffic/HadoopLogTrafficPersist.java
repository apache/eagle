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

package org.apache.eagle.security.traffic;

import com.typesafe.config.Config;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class HadoopLogTrafficPersist implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopLogTrafficPersist.class);
    private static final String SINK_BATCH_SIZE = "dataSinkConfig.metricSinkBatchSize";
    private IEagleServiceClient client;
    private int batchSize;
    private List<TaggedLogAPIEntity> entityBucket = new ArrayList<>();

    public HadoopLogTrafficPersist(Config config) {
        this.batchSize = config.hasPath(SINK_BATCH_SIZE) ? config.getInt(SINK_BATCH_SIZE) : 1;
        this.client = new EagleServiceClientImpl(config);
    }

    public void emitMetric(GenericMetricEntity metricEntity) {
        entityBucket.add(metricEntity);
        if (entityBucket.size() < batchSize) {
            return;
        }

        try {
            GenericServiceAPIResponseEntity response = client.create(entityBucket);
            if (response.isSuccess()) {
                LOG.info("persist {} entities with the earliest time={}", entityBucket.size(), entityBucket.get(0).getTimestamp());
            } else {
                LOG.error("Service side error: {}", response.getException());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            entityBucket.clear();
        }
    }

    public void close() {
        try {
            if (client != null) {
                this.client.close();
            }
        } catch (IOException e) {
            LOG.error("Close client error: {}", e.getMessage(), e);
        }
    }

}
