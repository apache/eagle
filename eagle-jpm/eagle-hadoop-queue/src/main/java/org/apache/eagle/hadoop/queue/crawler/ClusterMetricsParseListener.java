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

/**
 *
 */
package org.apache.eagle.hadoop.queue.crawler;

import backtype.storm.spout.SpoutOutputCollector;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.eagle.common.DateTimeUtil;
import org.apache.eagle.dataproc.impl.storm.ValuesArray;
import org.apache.eagle.hadoop.queue.model.clusterMetrics.ClusterMetrics;
import org.apache.eagle.hadoop.queue.storm.HadoopQueueMessageId;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants;
import org.apache.eagle.log.entity.GenericMetricEntity;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.DataSource;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.DataType;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.MetricName;
import org.apache.eagle.hadoop.queue.common.HadoopClusterConstants.AggregateFunc;

import java.util.*;
import java.util.stream.Collectors;

public class ClusterMetricsParseListener {

	private String site;
	private SpoutOutputCollector collector;

	private long maxTimestamp;
	private Map<MetricKey, GenericMetricEntity> clusterMetricEntities = new HashMap<>();
	private Map<MetricKey, Integer> clusterMetricCounts = new HashMap<>();

	private final static long AGGREGATE_INTERVAL = DateTimeUtil.ONEMINUTE;
	private final static long HOLD_TIME_WINDOW = 2 * DateTimeUtil.ONEMINUTE;

	public ClusterMetricsParseListener(String site, SpoutOutputCollector collector){
		reset();
		this.site = site;
		this.collector = collector;
	}

	private void createMetric(String metricName, long timestamp, double value, HadoopClusterConstants.AggregateFunc aggFunc){
		if (timestamp > maxTimestamp) {
			maxTimestamp = timestamp;
		}
		timestamp = timestamp / AGGREGATE_INTERVAL * AGGREGATE_INTERVAL;
		MetricKey key = new MetricKey(metricName, timestamp);
		GenericMetricEntity entity = clusterMetricEntities.get(key);
		if (entity == null) {
			entity = new GenericMetricEntity();
			entity.setTags(buildMetricTags());
			entity.setTimestamp(timestamp);
			entity.setPrefix(metricName);
			entity.setValue(new double[]{0.0});
			clusterMetricEntities.put(key, entity);
		}
		if (clusterMetricCounts.get(key) == null){
			clusterMetricCounts.put(key, 0);
		}
		updateEntityAggValue(entity, aggFunc, value, clusterMetricCounts.get(key));
		clusterMetricCounts.put(key, clusterMetricCounts.get(key) + 1);
	}

	public void onMetric(ClusterMetrics metrics, long currentTimestamp) {
		createMetric(MetricName.HADOOP_CLUSTER_NUMPENDING_JOBS, currentTimestamp, metrics.getAppsPending(), AggregateFunc.MAX);
		createMetric(MetricName.HADOOP_CLUSTER_ALLOCATED_MEMORY, currentTimestamp, metrics.getAllocatedMB(), AggregateFunc.AVG);
		createMetric(MetricName.HADOOP_CLUSTER_TOTAL_MEMORY, currentTimestamp, metrics.getTotalMB(), AggregateFunc.MAX);
		createMetric(MetricName.HADOOP_CLUSTER_AVAILABLE_MEMORY, currentTimestamp, metrics.getAvailableMB(), AggregateFunc.AVG);
		createMetric(MetricName.HADOOP_CLUSTER_RESERVED_MEMORY, currentTimestamp, metrics.getReservedMB(), AggregateFunc.AVG);
	}

	public void flush() {
		HadoopQueueMessageId messageId = new HadoopQueueMessageId(DataType.METRIC, DataSource.CLUSTER_METRIC, System.currentTimeMillis());
		List<GenericMetricEntity> metrics = new ArrayList<>(clusterMetricEntities.values());
		this.collector.emit(new ValuesArray(DataType.METRIC.name(), metrics), messageId);
		reset();
	}

	private void reset() {
		maxTimestamp = 0;
		clearOldCache();
	}

	private void clearOldCache() {
		List<MetricKey> removedkeys = clusterMetricEntities.keySet().stream().filter(key -> key.createTime < maxTimestamp - HOLD_TIME_WINDOW).collect(Collectors.toList());

		for (MetricKey key : removedkeys) {
			clusterMetricEntities.remove(key);
		}
	}

	private Map<String, String> buildMetricTags(){
		Map<String,String> tags = new HashMap<String, String>();
		tags.put(HadoopClusterConstants.TAG_SITE, site);
		return tags;
	}

	private void updateEntityAggValue(GenericMetricEntity entity,
									  HadoopClusterConstants.AggregateFunc aggFunc,
									  double value,
									  double count) {
		double lastValue = entity.getValue()[0];
		switch (aggFunc){
			case MAX:
				entity.setValue(new double[]{Math.max(lastValue, value)});
				return;
			case AVG:
				long avgValue = (long) ((lastValue * count + value) / (count +1));
				entity.setValue(new double[]{avgValue});
				return;
		}
	}

	private class MetricKey {
		String metricName;
		Long createTime;

		public MetricKey(String metricName, Long timestamp) {
			this.metricName = metricName;
			this.createTime = timestamp;
		}

		public boolean equals(Object obj) {
			if (obj instanceof MetricKey) {
				MetricKey key = (MetricKey) obj;
				if (key == null) {
					return false;
				}
				return Objects.equals(metricName, key.metricName) & Objects.equals(createTime, key.createTime);
			}
			return false;
		}

		public int hashCode() {
			return new HashCodeBuilder().append(metricName).append(createTime).toHashCode();
		}

	}
}
