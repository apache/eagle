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

package org.apache.eagle.hadoop.queue.crawler;

import org.apache.eagle.dataproc.impl.storm.ValuesArray;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.MetricName;
import org.apache.eagle.hadoop.queue.model.scheduler.*;
import org.apache.eagle.hadoop.queue.storm.HadoopQueueMessageId;
import org.apache.eagle.log.entity.GenericMetricEntity;

import backtype.storm.spout.SpoutOutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SchedulerInfoParseListener {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerInfoParseListener.class);
    //private final static long AGGREGATE_INTERVAL = DateTimeUtil.ONEMINUTE;
    //private int MAX_CACHE_COUNT = 1000;

    private final List<RunningQueueAPIEntity> runningQueueAPIEntities = new ArrayList<>();
    private final List<GenericMetricEntity> metricEntities = new ArrayList<>();

    private String site;
    private SpoutOutputCollector collector;

    public SchedulerInfoParseListener(String site, SpoutOutputCollector collector) {
        this.site = site;
        this.collector = collector;
    }

    public void onMetric(SchedulerInfo scheduler, long currentTimestamp) throws Exception {
        Map<String, String> tags = buildMetricTags(null, null);
        createMetric(MetricName.HADOOP_CLUSTER_CAPACITY, tags, currentTimestamp, scheduler.getCapacity());
        createMetric(MetricName.HADOOP_CLUSTER_USED_CAPACITY, tags, currentTimestamp, scheduler.getUsedCapacity());
        for (Queue queue : scheduler.getQueues().getQueue()) {
            createQueues(queue, currentTimestamp, scheduler, null);
        }
    }

    public void flush() {
        LOG.info("Flushing {} RunningQueue metrics in memory", metricEntities.size());
        HadoopQueueMessageId messageId = new HadoopQueueMessageId(HadoopClusterConstants.DataType.METRIC, HadoopClusterConstants.DataSource.SCHEDULER, System.currentTimeMillis());
        List<GenericMetricEntity> metrics = new ArrayList<>(metricEntities);
        collector.emit(new ValuesArray(HadoopClusterConstants.DataType.METRIC.name(), metrics), messageId);

        LOG.info("Flushing {} RunningQueueEntities in memory", runningQueueAPIEntities.size());
        messageId = new HadoopQueueMessageId(HadoopClusterConstants.DataType.ENTITY, HadoopClusterConstants.DataSource.SCHEDULER, System.currentTimeMillis());
        List<RunningQueueAPIEntity> entities = new ArrayList<>(runningQueueAPIEntities);
        collector.emit(new ValuesArray(HadoopClusterConstants.DataType.ENTITY.name(), entities), messageId);

        runningQueueAPIEntities.clear();
        metricEntities.clear();
    }

    private Map<String, String> buildMetricTags(String queueName, String parentQueueName) {
        Map<String, String> tags = new HashMap<>();
        tags.put(HadoopClusterConstants.TAG_SITE, this.site);
        if (queueName != null) {
            tags.put(HadoopClusterConstants.TAG_QUEUE, queueName);
        }
        if (parentQueueName != null) {
            tags.put(HadoopClusterConstants.TAG_PARENT_QUEUE, parentQueueName);
        }
        return tags;
    }

    private void createMetric(String metricName, Map<String, String> tags, long timestamp, double value) throws Exception {
        GenericMetricEntity e = new GenericMetricEntity();
        e.setPrefix(metricName);
        e.setTimestamp(timestamp);
        e.setTags(tags);
        e.setValue(new double[] {value});
        this.metricEntities.add(e);
    }

    private void createQueues(Queue queue, long currentTimestamp, SchedulerInfo scheduler, String parentQueueName) throws Exception {
        RunningQueueAPIEntity _entity = new RunningQueueAPIEntity();
        Map<String, String> _tags = buildMetricTags(queue.getQueueName(), parentQueueName);
        _entity.setTags(_tags);
        _entity.setState(queue.getState());
        _entity.setScheduler(scheduler.getType());
        _entity.setAbsoluteCapacity(queue.getAbsoluteCapacity());
        _entity.setAbsoluteMaxCapacity(queue.getAbsoluteMaxCapacity());
        _entity.setAbsoluteUsedCapacity(queue.getAbsoluteUsedCapacity());
        _entity.setMemory(queue.getResourcesUsed().getMemory());
        _entity.setVcores(queue.getResourcesUsed().getvCores());
        _entity.setNumActiveApplications(queue.getNumApplications());
        _entity.setNumPendingApplications(queue.getNumPendingApplications());
        _entity.setMaxActiveApplications(queue.getMaxActiveApplications());
        _entity.setTimestamp(currentTimestamp);

        List<UserWrapper> userList = new ArrayList<>();
        if (queue.getUsers() != null && queue.getUsers().getUser() != null) {
            for (User user : queue.getUsers().getUser()) {
                userList.add(wrapUser(user));
            }
        }
        UserWrappers users = new UserWrappers();
        users.setUsers(userList);
        _entity.setUsers(users);

        runningQueueAPIEntities.add(_entity);

        createMetric(MetricName.HADOOP_QUEUE_NUMPENDING_JOBS, _tags, currentTimestamp, queue.getNumPendingApplications());
        createMetric(MetricName.HADOOP_QUEUE_USED_CAPACITY, _tags, currentTimestamp, queue.getAbsoluteUsedCapacity());
        if (queue.getAbsoluteCapacity() == 0) {
            createMetric(MetricName.HADOOP_QUEUE_USED_CAPACITY_RATIO, _tags, currentTimestamp, 0);
        } else {
            createMetric(MetricName.HADOOP_QUEUE_USED_CAPACITY_RATIO, _tags, currentTimestamp, queue.getAbsoluteUsedCapacity() / queue.getAbsoluteCapacity());
        }

        if (queue.getUsers() != null && queue.getUsers().getUser() != null) {
            for (User user : queue.getUsers().getUser()) {
                Map<String, String> userTags = new HashMap<>(_tags);
                userTags.put(HadoopClusterConstants.TAG_USER, user.getUsername());
                createMetric(HadoopClusterConstants.MetricName.HADOOP_USER_NUMPENDING_JOBS, userTags, currentTimestamp, user.getNumPendingApplications());
                createMetric(HadoopClusterConstants.MetricName.HADOOP_USER_USED_MEMORY, userTags, currentTimestamp, user.getResourcesUsed().getMemory());
                createMetric(HadoopClusterConstants.MetricName.HADOOP_USER_USED_MEMORY_RATIO, userTags, currentTimestamp,
                    ((double) user.getResourcesUsed().getMemory()) / queue.getResourcesUsed().getMemory());
            }
        }

        if (queue.getQueues() != null && queue.getQueues().getQueue() != null) {
            for (Queue subQueue : queue.getQueues().getQueue()) {
                createQueues(subQueue, currentTimestamp, scheduler, queue.getQueueName());
            }
        }
    }

    private UserWrapper wrapUser(User user) {
        UserWrapper wrapper = new UserWrapper();
        wrapper.setUsername(user.getUsername());
        wrapper.setMemory(user.getResourcesUsed().getMemory());
        wrapper.setvCores(user.getResourcesUsed().getvCores());
        wrapper.setNumActiveApplications(user.getNumActiveApplications());
        wrapper.setNumPendingApplications(user.getNumPendingApplications());
        return wrapper;
    }
}
