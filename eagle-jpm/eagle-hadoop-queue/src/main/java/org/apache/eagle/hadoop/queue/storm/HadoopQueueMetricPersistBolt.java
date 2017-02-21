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
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.DataSource;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.DataType;
import org.apache.eagle.hadoop.queue.model.applications.AppStreamInfo;
import org.apache.eagle.hadoop.queue.model.applications.YarnAppAPIEntity;
import org.apache.eagle.hadoop.queue.model.scheduler.QueueStreamInfo;
import org.apache.eagle.hadoop.queue.model.scheduler.RunningQueueAPIEntity;
import org.apache.eagle.log.base.taggedlog.TaggedLogAPIEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HadoopQueueMetricPersistBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopQueueMetricPersistBolt.class);

    private Map<HadoopClusterConstants.DataSource, String> streamMap;
    private HadoopQueueRunningAppConfig config;
    private IEagleServiceClient client;
    private OutputCollector collector;

    public HadoopQueueMetricPersistBolt(HadoopQueueRunningAppConfig config,
                                        Map<HadoopClusterConstants.DataSource, String> streamMap) {
        this.config = config;
        this.streamMap = streamMap;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        HadoopQueueRunningAppConfig.EagleProps.EagleService eagleService = config.eagleProps.eagleService;
        this.client = new EagleServiceClientImpl(
                eagleService.host,
                eagleService.port,
                eagleService.username,
                eagleService.password);
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input == null) {
            return;
        }
        DataSource dataSource = (DataSource) input.getValueByField(HadoopClusterConstants.FIELD_DATASOURCE);
        DataType dataType = (DataType) input.getValueByField(HadoopClusterConstants.FIELD_DATATYPE);
        Object data = input.getValueByField(HadoopClusterConstants.FIELD_DATA);

        List<TaggedLogAPIEntity> entities = (List<TaggedLogAPIEntity>) data;
        if (dataType.equals(DataType.METRIC)) {
            writeEntities(entities, dataType, dataSource);
        } else {
            for (TaggedLogAPIEntity entity : entities) {
                if (entity instanceof RunningQueueAPIEntity) {
                    RunningQueueAPIEntity queue = (RunningQueueAPIEntity) entity;
                    if (queue.getUsers() != null && !queue.getUsers().getUsers().isEmpty() && queue.getMemory() != 0) {
                        String queueName = queue.getTags().get(HadoopClusterConstants.TAG_QUEUE);
                        collector.emit(streamMap.get(dataSource),
                                new Values(queueName, QueueStreamInfo.convertEntityToStream(queue)));
                    }
                } else if (entity instanceof YarnAppAPIEntity) {
                    YarnAppAPIEntity appAPIEntity = (YarnAppAPIEntity) entity;
                    collector.emit(streamMap.get(dataSource),
                            new Values(appAPIEntity.getAppName(), AppStreamInfo.convertAppToStream(appAPIEntity)));
                }
            }
            if (!dataSource.equals(DataSource.RUNNING_APPS)) {
                writeEntities(entities, dataType, dataSource);
            }
        }
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (streamMap != null) {
            for (String stormStreamId : streamMap.values()) {
                declarer.declareStream(stormStreamId, new Fields("f1", "message"));
            }
        } else {
            declarer.declare(new Fields("f1", "message"));
        }
    }

    @Override
    public void cleanup() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private void writeEntities(List<TaggedLogAPIEntity> entities, DataType dataType, DataSource dataSource) {
        try {
            GenericServiceAPIResponseEntity response = client.create(entities);
            if (!response.isSuccess()) {
                LOG.error("Got exception from eagle service: " + response.getException());
            } else {
                LOG.info("Successfully wrote {} items of {} for {}", entities.size(), dataType, dataSource);
            }
        } catch (Exception e) {
            LOG.error("cannot create {} entities", entities.size(), e);
        }
        entities.clear();
    }
}
