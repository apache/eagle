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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;
import org.apache.eagle.hadoop.jmx.model.HadoopHAResult;
import org.apache.eagle.hadoop.jmx.model.JmxMetricEntity;
import org.apache.eagle.hadoop.jmx.model.MetricEntity;

import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.PLACE_HOLDER;

public abstract class HadoopHAStateChecker {
    private String site;
    private String component;
    protected SpoutOutputCollector collector;

    public HadoopHAStateChecker(String site, String component, SpoutOutputCollector collector) {
        this.site = site;
        this.component = component;
        this.collector = collector;
    }
    /**
     * check ha state and emit the parsed jmx object into downstream processing bolt
     *
     * @param urls are the web url
     * @param downStreamId the shuffle stream id for the downstream processors
     * @param metricStreamId the metric stream id for the kafka sink
     */
    public abstract void checkAndEmit(String[] urls,
                                      String downStreamId,
                                      String metricStreamId);

    protected void emit(HadoopHAResult result, String metricStreamId) {
        long timestamp = System.currentTimeMillis();
        JmxMetricEntity metricEntity = new JmxMetricEntity();
        metricEntity.setHost(result.host);
        metricEntity.setTimestamp(timestamp);
        metricEntity.setSite(site);

        emit(metricStreamId, metricEntity, String.format(HadoopJmxConstant.HA_TOTAL_FORMAT, component),
                result.total_count);
        emit(metricStreamId, metricEntity, String.format(HadoopJmxConstant.HA_ACTIVE_FORMAT, component),
                result.active_count);
        emit(metricStreamId, metricEntity, String.format(HadoopJmxConstant.HA_STANDBY_FORMAT, component),
                result.standby_count);
        emit(metricStreamId, metricEntity, String.format(HadoopJmxConstant.HA_FAILED_FORMAT, component),
                result.failed_count);
    }

    private void emit(String metricStreamId, JmxMetricEntity entity, String metric, double value) {
        entity.setMetric(metric);
        entity.setValue(value);
        collector.emit(metricStreamId, new Values(PLACE_HOLDER, entity.buildStream()));
    }

}
