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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.eagle.hadoop.queue.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.eagle.hadoop.queue.HadoopQueueRunningAppConfig;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.LeafQueueInfo;
import org.apache.eagle.hadoop.queue.model.scheduler.RunningQueueAPIEntity;
import org.apache.eagle.hadoop.queue.model.scheduler.UserWrapper;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HadoopQueueMetricPersistBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopQueueMetricPersistBolt.class);

    private HadoopQueueRunningAppConfig config;
    private IEagleServiceClient client;
    private OutputCollector collector;

    public HadoopQueueMetricPersistBolt(HadoopQueueRunningAppConfig config) {
        this.config = config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        HadoopQueueRunningAppConfig.EagleProps.EagleService eagleService = config.eagleProps.eagleService;
        this.client = new EagleServiceClientImpl(eagleService.host, eagleService.port, eagleService.username, eagleService.password);
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input == null) {
            return;
        }
        String dataType = input.getStringByField(HadoopClusterConstants.FIELD_DATATYPE);
        Object data = input.getValueByField(HadoopClusterConstants.FIELD_DATA);
        if (dataType.equalsIgnoreCase(HadoopClusterConstants.DataType.METRIC.toString())) {
            List<GenericMetricEntity> metrics = (List<GenericMetricEntity>) data;
            writeMetrics(metrics);
        } else if (dataType.equalsIgnoreCase(HadoopClusterConstants.DataType.ENTITY.toString())) {
            List<RunningQueueAPIEntity> entities = (List<RunningQueueAPIEntity>) data;
            for (RunningQueueAPIEntity queue : entities) {
                if (queue.getUsers() != null && !queue.getUsers().isEmpty() && queue.getMemory() != 0) {
                    collector.emit(new Values(queue.getTags().get(HadoopClusterConstants.TAG_QUEUE), parseLeafQueueInfo(queue)));
                }
            }
            writeEntities(entities);
        }
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(HadoopClusterConstants.LeafQueueInfo.QUEUE_NAME, "message"));
    }

    private void writeEntities(List<RunningQueueAPIEntity> entities) {
        try {
            GenericServiceAPIResponseEntity response = client.create(entities);
            if (!response.isSuccess()) {
                LOG.error("Got exception from eagle service: " + response.getException());
            } else {
                LOG.info("Successfully wrote " + entities.size() + " RunningQueueAPIEntity entities");
            }
        } catch (Exception e) {
            LOG.error("cannot create running queue entities successfully", e);
        }
        entities.clear();
    }

    private void writeMetrics(List<GenericMetricEntity> entities) {
        try {
            GenericServiceAPIResponseEntity response = client.create(entities);
            if (response.isSuccess()) {
                LOG.info("Successfully wrote " + entities.size() + " GenericMetricEntity entities");
            } else {
                LOG.error(response.getException());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private Map<String, Object> parseLeafQueueInfo(RunningQueueAPIEntity queueAPIEntity) {
        Map<String, Object> queueInfoMap = new HashMap<>();
        queueInfoMap.put(LeafQueueInfo.QUEUE_SITE, queueAPIEntity.getTags().get(HadoopClusterConstants.TAG_SITE));
        queueInfoMap.put(LeafQueueInfo.QUEUE_NAME, queueAPIEntity.getTags().get(HadoopClusterConstants.TAG_QUEUE));
        queueInfoMap.put(LeafQueueInfo.QUEUE_ABSOLUTE_CAPACITY, queueAPIEntity.getAbsoluteCapacity());
        queueInfoMap.put(LeafQueueInfo.QUEUE_ABSOLUTE_MAX_CAPACITY, queueAPIEntity.getAbsoluteMaxCapacity());
        queueInfoMap.put(LeafQueueInfo.QUEUE_ABSOLUTE_USED_CAPACITY, queueAPIEntity.getAbsoluteUsedCapacity());
        queueInfoMap.put(LeafQueueInfo.QUEUE_MAX_ACTIVE_APPS, queueAPIEntity.getMaxActiveApplications());
        queueInfoMap.put(LeafQueueInfo.QUEUE_NUM_ACTIVE_APPS, queueAPIEntity.getNumActiveApplications());
        queueInfoMap.put(LeafQueueInfo.QUEUE_NUM_PENDING_APPS, queueAPIEntity.getNumPendingApplications());
        queueInfoMap.put(LeafQueueInfo.QUEUE_SCHEDULER, queueAPIEntity.getScheduler());
        queueInfoMap.put(LeafQueueInfo.QUEUE_STATE, queueAPIEntity.getState());
        queueInfoMap.put(LeafQueueInfo.QUEUE_USED_MEMORY, queueAPIEntity.getMemory());
        queueInfoMap.put(LeafQueueInfo.QUEUE_USED_VCORES, queueAPIEntity.getVcores());

        double maxUserUsedCapacity = 0;
        for (UserWrapper user : queueAPIEntity.getUsers()) {
            double userUsedCapacity = calculateUserUsedCapacity(
                    queueAPIEntity.getAbsoluteUsedCapacity(),
                    queueAPIEntity.getMemory(),
                    user.getMemory());
            if (userUsedCapacity > maxUserUsedCapacity) {
                maxUserUsedCapacity = userUsedCapacity;
            }

        }
        queueInfoMap.put(LeafQueueInfo.QUEUE_MAX_USER_USED_CAPACITY, maxUserUsedCapacity);
        return queueInfoMap;
    }

    private double calculateUserUsedCapacity(double absoluteUsedCapacity, long queueUsedMem, long userUsedMem) {
        return userUsedMem * absoluteUsedCapacity / queueUsedMem;
    }
}
