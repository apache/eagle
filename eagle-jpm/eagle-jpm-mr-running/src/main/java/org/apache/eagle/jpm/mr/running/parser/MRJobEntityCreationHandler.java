/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.mr.running.parser;

import org.apache.eagle.jpm.mr.running.config.MRRunningConfigManager;
import org.apache.eagle.jpm.mr.running.entities.JobExecutionAPIEntity;
import org.apache.eagle.jpm.mr.running.entities.TaskExecutionAPIEntity;
import org.apache.eagle.jpm.mr.running.parser.metrics.JobExecutionMetricsCreationListener;
import org.apache.eagle.jpm.mr.running.parser.metrics.TaskExecutionMetricsCreationListener;
import org.apache.eagle.jpm.util.Utils;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

public class MRJobEntityCreationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(MRJobEntityCreationHandler.class);

    private List<TaggedLogAPIEntity> entities = new ArrayList<>();
    private MRRunningConfigManager.EagleServiceConfig eagleServiceConfig;
    private JobExecutionMetricsCreationListener jobMetricsListener;
    private TaskExecutionMetricsCreationListener taskMetricsListener;

    public MRJobEntityCreationHandler(MRRunningConfigManager.EagleServiceConfig eagleServiceConfig) {
        this.eagleServiceConfig = eagleServiceConfig;
        jobMetricsListener = new JobExecutionMetricsCreationListener();
        taskMetricsListener = new TaskExecutionMetricsCreationListener();
    }

    public void add(TaggedLogAPIEntity entity) {
        entities.add(entity);
        List<GenericMetricEntity> metricEntities;
        if (entity instanceof TaskExecutionAPIEntity) {
            metricEntities = taskMetricsListener.generateMetrics((TaskExecutionAPIEntity) entity);
            entities.addAll(metricEntities);
        } else if (entity instanceof JobExecutionAPIEntity) {
            metricEntities = jobMetricsListener.generateMetrics((JobExecutionAPIEntity) entity);
            entities.addAll(metricEntities);
        }
        if (entities.size() >= eagleServiceConfig.maxFlushNum) {
            this.flush();
        }
    }

    public boolean flush() {
        //need flush right now
        if (entities.size() == 0) {
            return true;
        }
        IEagleServiceClient client = new EagleServiceClientImpl(
                eagleServiceConfig.eagleServiceHost,
                eagleServiceConfig.eagleServicePort,
                eagleServiceConfig.username,
                eagleServiceConfig.password);
        client.getJerseyClient().setReadTimeout(eagleServiceConfig.readTimeoutSeconds * 1000);
        try {
            LOG.info("start to flush mr job entities, size {}", entities.size());
            client.create(entities);
            LOG.info("finish flushing mr job entities, size {}", entities.size());
            entities.clear();
        } catch (Exception e) {
            LOG.warn("exception found when flush entities, {}", e);
            e.printStackTrace();
            return false;
        } finally {
            client.getJerseyClient().destroy();
            try {
                client.close();
            } catch (Exception e) {
            }
        }

        return true;
    }
}
