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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.FS_HASTATE_TAG;
import static org.apache.eagle.hadoop.jmx.HadoopJmxConstant.FS_HOSTNAME_TAG;

public class NamenodeHAStateChecker extends HadoopHAStateChecker {
    private static final Logger LOG = LoggerFactory.getLogger(NamenodeHAStateChecker.class);

    public NamenodeHAStateChecker(String site, String component, SpoutOutputCollector collector) {
        super(site, component, collector);
    }

    @Override
    public void checkAndEmit(String[] urls, String downStreamId, String metricStreamId) {
        HadoopHAResult hadoopHAResult = new HadoopHAResult();
        List<String> hosts = new ArrayList<>();

        for (String url : urls) {
            try {
                final Map<String, JMXBean> jmxBeanMap = JMXQueryHelper.query(url);

                collector.emit(downStreamId, new Values(jmxBeanMap));
                JMXBean bean = jmxBeanMap.get(HadoopJmxConstant.FSNAMESYSTEM_BEAN);
                if (bean.getPropertyMap().containsKey(FS_HASTATE_TAG)) {
                    String haState = (String) bean.getPropertyMap().get(FS_HASTATE_TAG);
                    LOG.debug("{} is found from {}", FS_HASTATE_TAG, url);
                    if (haState.equalsIgnoreCase(HadoopJmxConstant.ACTIVE_STATE)) {
                        hadoopHAResult.active_count += 1;
                    } else {
                        hadoopHAResult.standby_count += 1;
                    }
                } else {
                    LOG.info("{} is NOT found from {}", FS_HASTATE_TAG, url);
                }
                if (bean.getPropertyMap().containsKey(FS_HOSTNAME_TAG)) {
                    hosts.add((String) bean.getPropertyMap().get(FS_HOSTNAME_TAG));
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                hadoopHAResult.failed_count += 1;
            }
            hadoopHAResult.total_count = urls.length;
            hadoopHAResult.host = String.join(",", hosts);
        }
        emit(hadoopHAResult, metricStreamId);
    }

}
