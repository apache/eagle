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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.eagle.hadoop.jmx.model.JmxMetricEntity;
import org.apache.eagle.hadoop.jmx.HadoopJmxConstant.MetricType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class NamenodeMetricProcessor extends DefinedMetricProcessor {
    private final Logger LOG = LoggerFactory.getLogger(NamenodeMetricProcessor.class);
    private JmxMetricCollector collector;

    public NamenodeMetricProcessor(JmxMetricCollector collector) {
        this.collector = collector;
    }

    public void registerListener() {
        registerListener(new FsNameSystemBeanListener());
        registerListener(new FsNameSystemStateBeanListener());
        registerListener(new NameNodeInfoBeanListener());
        registerListener(new JvmMetricBeanListener());
    }

    public double buildUsageMetric(Map<String, Object> kvs, String first, String second) {
        double firstVal = (double) kvs.get(first);
        double secondVal = (double) kvs.get(second);
        try {
            return Double.valueOf(String.format(".2f", firstVal * 100 / secondVal));
        } catch (Exception e) {
            LOG.error("Exception is caught for metric {} due to {}", secondVal, e.getMessage());
            return 0;
        }
    }

    private class FsNameSystemStateBeanListener implements JmxBeanListener {

        @Override
        public void on_bean(JmxMetricEntity baseMetric, Map.Entry<String, JMXBean> bean) {
            if (bean.getKey().equals(HadoopJmxConstant.FSNAMESYSTEMSTATE_BEAN)) {
                String PREFIX = "hadoop.namenode.fsnamesystemstate";
                Map<String, Object> kvs = bean.getValue().getPropertyMap();
                if (kvs.containsKey("FSState")) {
                    double value = 0;
                    if (Objects.equals(kvs.get("FSState"), "safeMode")) {
                        value = 1;
                    }
                    collector.emit(baseMetric, PREFIX + ".fsstate", value);
                }

                if (kvs.containsKey("CapacityUsed") && kvs.containsKey("CapacityTotal")) {
                    collector.emit(baseMetric, PREFIX + ".capacityUsage",
                            buildUsageMetric(kvs, "CapacityUsed", "CapacityTotal"));
                }
            }
        }
    }

    private class FsNameSystemBeanListener implements JmxBeanListener {

        @Override
        public void on_bean(JmxMetricEntity baseMetric, Map.Entry<String, JMXBean> bean) {
            if (bean.getKey().equals(HadoopJmxConstant.FSNAMESYSTEM_BEAN)) {
                String PREFIX = "hadoop.namenode.fsnamesystem";
                Map<String, Object> kvs = bean.getValue().getPropertyMap();
                if (kvs.containsKey("tag.HAState")) {
                    double value = 1;
                    if (Objects.equals(kvs.get("tag.HAState"), "active")) {
                        value = 0;
                    }
                    collector.emit(baseMetric, PREFIX + ".hastate", value, MetricType.METRIC);
                }
            }
        }
    }

    private class NameNodeInfoBeanListener implements JmxBeanListener {
        private final Logger LOG = LoggerFactory.getLogger(NameNodeInfoBeanListener.class);

        @Override
        public void on_bean(JmxMetricEntity baseMetric, Map.Entry<String, JMXBean> bean) {
            if (bean.getKey().equals(HadoopJmxConstant.NAMENODEINFO_BEAN)) {
                String metric = "hadoop.namenode.namenodeinfo.corruptfiles";
                Map<String, Object> kvs = bean.getValue().getPropertyMap();
                if (kvs.containsKey("CorruptFiles")) {
                    collector.emit(baseMetric, metric, kvs.get("CorruptFiles"), MetricType.RESOURCE);
                }

                String PREFIX = "hadoop.namenode.journaltransaction";
                if (kvs.containsKey("JournalTransactionInfo")) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    Map<String, Double> map = null;
                    try {
                        map = objectMapper.readValue(kvs.get("JournalTransactionInfo").toString(), Map.class);
                    } catch (IOException e) {
                        LOG.error("Json parser exception: {}", e.getMessage(), e);
                    }
                    if (map != null) {
                        collector.emit(baseMetric, PREFIX + ".LastAppliedOrWrittenTxId", map.get("LastAppliedOrWrittenTxId"));
                        collector.emit(baseMetric, PREFIX + ".MostRecentCheckpointTxId", map.get("MostRecentCheckpointTxId"));
                    }
                }
            }
        }
    }

    private class JvmMetricBeanListener implements JmxBeanListener {

        @Override
        public void on_bean(JmxMetricEntity baseMetric, Map.Entry<String, JMXBean> bean) {
            if (bean.getKey().equals("Hadoop:service=NameNode,name=JvmMetrics")) {
                Map<String, Object> kvs = bean.getValue().getPropertyMap();
                String PREFIX = "hadoop.namenode.jvm";

                if (kvs.containsKey("'MemNonHeapUsedM'") && kvs.containsKey("'MemNonHeapMaxM'")) {
                    collector.emit(baseMetric, PREFIX + ".memnonheapusedusage",
                            buildUsageMetric(kvs, "MemNonHeapUsedM", "MemNonHeapMaxM"));
                    collector.emit(baseMetric, PREFIX + ".memnonheapcommittedusage",
                            buildUsageMetric(kvs, "MemNonHeapCommittedM", "MemNonHeapMaxM"));
                    collector.emit(baseMetric, PREFIX + ".memheapusedusage",
                            buildUsageMetric(kvs, "MemHeapUsedM", "MemHeapMaxM"));
                    collector.emit(baseMetric, PREFIX + ".memheapcommittedusage",
                            buildUsageMetric(kvs, "MemHeapCommittedM", "MemHeapMaxM"));
                }
            }
        }
    }


 }
