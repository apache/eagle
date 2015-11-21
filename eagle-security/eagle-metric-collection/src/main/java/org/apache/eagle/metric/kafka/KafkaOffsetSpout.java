/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metric.kafka;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.typesafe.config.Config;
import org.apache.eagle.common.config.EagleConfigConstants;
import org.apache.eagle.dataproc.impl.storm.zookeeper.ZKStateConfig;
import org.apache.eagle.metric.CountingMetric;
import org.apache.eagle.metric.Metric;
import org.apache.eagle.metric.manager.EagleMetricReportManager;
import org.apache.eagle.metric.report.EagleServiceMetricReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaOffsetSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private static final long DEFAULT_ROUND_INTERVALS = 5 * 60 * 1000;
	private KafkaOffsetCheckerConfig config;
	private KafkaConsumerOffsetFetcher consumerOffsetFetcher;
	private KafkaLatestOffsetFetcher latestOffsetFetcher;
	private Map<String, String> baseMetricDimension;
	private long lastRoundTime = 0;

	private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetSpout.class);

	public KafkaOffsetSpout(KafkaOffsetCheckerConfig config) {//Config config, ZKStateConfig zkStateConfig, String kafkaEndPoints){
		this.config = config;
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		consumerOffsetFetcher = new KafkaConsumerOffsetFetcher(config.zkConfig, config.kafkaConfig.topic, config.kafkaConfig.group);
		latestOffsetFetcher = new KafkaLatestOffsetFetcher(config.kafkaConfig.kafkaEndPoints);

		this.baseMetricDimension = new HashMap<>();
		this.baseMetricDimension.put("site", config.kafkaConfig.site);
		this.baseMetricDimension.put("topic", config.kafkaConfig.topic);
		this.baseMetricDimension.put("group", config.kafkaConfig.group);
		String eagleServiceHost = config.serviceConfig.serviceHost;
		Integer eagleServicePort = config.serviceConfig.servicePort;
		String username = config.serviceConfig.serviceHost;
		String password = config.serviceConfig.serviceHost;
		EagleServiceMetricReport report = new EagleServiceMetricReport(eagleServiceHost, eagleServicePort, username, password);
		EagleMetricReportManager.getInstance().register("metricCollectServiceReport", report);
	}

	public Metric constructMetric(long timestamp, String partition, double value) {
		Map<String, String> dimensions = new HashMap<>();
		dimensions.putAll(baseMetricDimension);
		dimensions.put("partition", partition);
		String metricName = "eagle.kafka.message.consumer.lag";
		Metric metric = new CountingMetric(timestamp, dimensions, metricName, value);
		return metric;
	}

	@Override
	public void nextTuple() {
		Long currentTime = System.currentTimeMillis();
		if (currentTime - lastRoundTime > DEFAULT_ROUND_INTERVALS) {
			try {
				Map<String, Long> consumedOffset = consumerOffsetFetcher.fetch();
				Map<Integer, Long> latestOffset = latestOffsetFetcher.fetch(config.kafkaConfig.topic, consumedOffset.size());
				List<Metric> list = new ArrayList<>();
				for (Map.Entry<String, Long> entry : consumedOffset.entrySet()) {
					String partition = entry.getKey();
					Integer partitionNumber = Integer.valueOf(partition.split("_")[1]);
					Long lag = latestOffset.get(partitionNumber) - entry.getValue();
					list.add(constructMetric(currentTime, partition, lag));
				}
				EagleMetricReportManager.getInstance().emit(list);
			} catch (Exception ex) {
				LOG.error("Got an exception, ex: ", ex);
			}
		}
        try{
            Thread.sleep(10 * 1000);
        }catch(Throwable t){
			//Do nothing
        }
    }
	
	/**
	 * empty because framework will take care of output fields declaration
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }
   
    @Override
    public void deactivate() {
    	
    }
   
    @Override
    public void close() {
    	
    }
}
