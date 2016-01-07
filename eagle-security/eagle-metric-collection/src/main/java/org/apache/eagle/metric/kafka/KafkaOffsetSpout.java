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
import org.apache.eagle.metric.reportor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaOffsetSpout extends BaseRichSpout {
	private static final long serialVersionUID = 1L;
	private static final long DEFAULT_ROUND_INTERVALS = 60 * 1000;
	private KafkaOffsetCheckerConfig config;
	private KafkaConsumerOffsetFetcher consumerOffsetFetcher;
	private KafkaLatestOffsetFetcher latestOffsetFetcher;
	private Map<String, String> baseMetricDimension;
	private long lastRoundTime = 0;
	private EagleMetricListener listener;

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
		String host = config.serviceConfig.serviceHost;
		Integer port = config.serviceConfig.servicePort;
		String username = config.serviceConfig.username;
		String password = config.serviceConfig.password;
		listener = new EagleServiceReporterMetricListener(host, port, username, password);
	}

	private EagleMetric constructMetric(long timestamp, String partition, double value) {
		Map<String, String> dimensions = new HashMap<>();
		dimensions.putAll(baseMetricDimension);
		dimensions.put("partition", partition);
		String metricName = "eagle.kafka.message.consumer.lag";
		String metricKey = MetricKeyCodeDecoder.codeTSMetricKey(timestamp, metricName, dimensions);
		EagleGaugeMetric metric = new EagleGaugeMetric(timestamp, metricKey, value);
		return metric;
	}

	private long trimTimestamp(long currentTime, long granularity) {
		return currentTime / granularity * granularity;
	}

	@Override
	public void nextTuple() {
		Long currentTime = System.currentTimeMillis();
		if (currentTime - lastRoundTime > DEFAULT_ROUND_INTERVALS) {
			try {
				long trimCurrentTime = trimTimestamp(currentTime, DEFAULT_ROUND_INTERVALS);
				Map<String, Long> consumedOffset = consumerOffsetFetcher.fetch();
				Map<Integer, Long> latestOffset = latestOffsetFetcher.fetch(config.kafkaConfig.topic, consumedOffset.size());
				List<EagleMetric> metrics = new ArrayList<>();
				for (Map.Entry<String, Long> entry : consumedOffset.entrySet()) {
					String partition = entry.getKey();
					Integer partitionNumber = Integer.valueOf(partition.split("_")[1]);
					Long lag = latestOffset.get(partitionNumber) - entry.getValue();
					// If the partition is not available
					if (latestOffset.get(partitionNumber) == -1) lag = -1L;
					EagleMetric metric = constructMetric(trimCurrentTime, partition, lag);
					metrics.add(metric);
				}
				lastRoundTime = trimCurrentTime;
				listener.onMetricFlushed(metrics);
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
