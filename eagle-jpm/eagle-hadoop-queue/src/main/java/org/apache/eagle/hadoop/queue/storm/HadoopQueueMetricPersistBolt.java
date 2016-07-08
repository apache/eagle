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

package org.apache.eagle.hadoop.queue.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.typesafe.config.Config;
import org.apache.eagle.hadoop.queue.model.scheduler.RunningQueueAPIEntity;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.log.entity.GenericServiceAPIResponseEntity;
import org.apache.eagle.service.client.EagleServiceConnector;
import org.apache.eagle.service.client.IEagleServiceClient;
import org.apache.eagle.service.client.impl.EagleServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class HadoopQueueMetricPersistBolt extends BaseRichBolt {

    private final static Logger LOG = LoggerFactory.getLogger(HadoopQueueMetricPersistBolt.class);

    private Config config;
    private IEagleServiceClient client;
    private OutputCollector collector;

    public HadoopQueueMetricPersistBolt(Config config) {
        this.config = config;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.client = new EagleServiceClientImpl(new EagleServiceConnector(this.config));
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
            writeEntities(entities);
        }
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private void writeEntities(List<RunningQueueAPIEntity> entities){
        try {
            GenericServiceAPIResponseEntity response = client.create(entities);
            if(!response.isSuccess()){
                LOG.error("Got exception from eagle service: " + response.getException());
            }else{
                LOG.info("Successfully wrote " + entities.size() + " RunningQueueAPIEntity entities");
            }
        } catch (Exception e) {
            LOG.error("cannot create running queue entities successfully", e);
        }
        entities.clear();
    }

    private void writeMetrics(List<GenericMetricEntity> entities){
        try {
            GenericServiceAPIResponseEntity response = client.create(entities);
            if(response.isSuccess()){
                LOG.info("Successfully wrote " + entities.size() + " GenericMetricEntity entities");
            }else{
                LOG.error(response.getException());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }


}
