# !/usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from metric_collector import JmxMetricCollector,JmxMetricListener,Runner,MetricNameConverter
import json, logging, fnmatch, sys, time

class NNSafeModeMetric(JmxMetricListener):
    def on_metric(self, metric):
        if metric["metric"] == "hadoop.namenode.fsnamesystemstate.fsstate":
            if metric["value"] == "safeMode":
                metric["value"] = 1
            else:
                metric["value"] = 0
            self.collector.collect(metric)

class NNFileSystemMetric(JmxMetricListener):
    PREFIX = "hadoop.namenode.dfs"

    def on_bean(self, component, bean):
        if bean["name"] == "Hadoop:service=NameNode,name=FSNamesystem":
            checkpointtimelag = int(round(time.time() * 1000)) - bean['LastCheckpointTime']
            self.collector.on_bean_kv(self.PREFIX, component, "checkpointtimelag", checkpointtimelag)

class corruptfilesMetric(JmxMetricListener):
    def on_metric(self, metric):
        if metric["metric"] == "hadoop.namenode.namenodeinfo.corruptfiles":
            self.collector.collect(metric, "string", MetricNameConverter())

class TopUserOpCountsMetric(JmxMetricListener):
    def on_metric(self, metric):
        if metric["metric"] == "hadoop.namenode.fsnamesystemstate.topuseropcounts":
            self.collector.collect(metric, "string", MetricNameConverter())

class MemoryUsageMetric(JmxMetricListener):
    PREFIX = "hadoop.namenode.jvm"

    def on_bean(self, component, bean):
        if bean["name"] == "Hadoop:service=NameNode,name=JvmMetrics":
            memnonheapusedusage = round(float(bean['MemNonHeapUsedM']) / float(bean['MemNonHeapMaxM']) * 100.0, 2)
            self.collector.on_bean_kv(self.PREFIX, component, "memnonheapusedusage", memnonheapusedusage)
            memnonheapcommittedusage = round(float(bean['MemNonHeapCommittedM']) / float(bean['MemNonHeapMaxM']) * 100,
                                             2)
            self.collector.on_bean_kv(self.PREFIX, component, "memnonheapcommittedusage", memnonheapcommittedusage)
            memheapusedusage = round(float(bean['MemHeapUsedM']) / float(bean['MemHeapMaxM']) * 100, 2)
            self.collector.on_bean_kv(self.PREFIX, component,"memheapusedusage", memheapusedusage)
            memheapcommittedusage = round(float(bean['MemHeapCommittedM']) / float(bean['MemHeapMaxM']) * 100, 2)
            self.collector.on_bean_kv(self.PREFIX, component, "memheapcommittedusage", memheapcommittedusage)

class NNCapacityUsageMetric(JmxMetricListener):
    PREFIX = "hadoop.namenode.fsnamesystemstate"

    def on_bean(self, component, bean):
        if bean["name"] == "Hadoop:service=NameNode,name=FSNamesystemState":
            capacityusage = round(float(bean['CapacityUsed']) / float(bean['CapacityTotal']) * 100, 2)
            self.collector.on_bean_kv(self.PREFIX, component, "capacityusage", capacityusage)

            numrevisedlivedatanodes = bean['NumLiveDataNodes'] + bean['NumDecomDeadDataNodes']
            self.collector.on_bean_kv(self.PREFIX, component, "numrevisedlivedatanodes", numrevisedlivedatanodes)

class JournalTransactionInfoMetric(JmxMetricListener):
    PREFIX = "hadoop.namenode.journaltransaction"

    def on_bean(self, component, bean):
        if bean.has_key("JournalTransactionInfo"):
            JournalTransactionInfo = json.loads(bean.get("JournalTransactionInfo"))
            LastAppliedOrWrittenTxId = float(JournalTransactionInfo.get("LastAppliedOrWrittenTxId"))
            MostRecentCheckpointTxId = float(JournalTransactionInfo.get("MostRecentCheckpointTxId"))
            self.collector.on_bean_kv(self.PREFIX, component, "LastAppliedOrWrittenTxId", LastAppliedOrWrittenTxId)
            self.collector.on_bean_kv(self.PREFIX, component, "MostRecentCheckpointTxId", MostRecentCheckpointTxId)

class DatanodeFSDatasetState(JmxMetricListener):
    def on_metric(self, metric):
        if fnmatch.fnmatch(metric["metric"], "hadoop.datanode.fsdatasetstate-*.capacity"):
            metric["metric"] = "hadoop.datanode.fsdatasetstate.capacity"
            self.collector.collect(metric)
        elif fnmatch.fnmatch(metric["metric"], "hadoop.datanode.fsdatasetstate-*.dfsused"):
            metric["metric"] = "hadoop.datanode.fsdatasetstate.dfsused"
            self.collector.collect(metric)

class HBaseRegionServerMetric(JmxMetricListener):
    def on_metric(self, metric):
        """
        Rename metric "hadoop.hbase.ipc.ipc.*" to "hadoop.hbase.regionserver.ipc.*" to support different hbase version metric
        """
        if fnmatch.fnmatch(metric["metric"],"hadoop.hbase.ipc.ipc.*") and metric["component"] == "regionserver":
            new_metric_name = metric["metric"].replace("hadoop.hbase.ipc.ipc.","hadoop.hbase.regionserver.ipc.")
            logging.debug("Rename metric %s to %s" % (metric["metric"], new_metric_name))
            metric["metric"] = new_metric_name
            self.collector.collect(metric)

if __name__ == '__main__':
    collector = JmxMetricCollector()
    collector.register(
        NNSafeModeMetric(),
        NNFileSystemMetric(),
        MemoryUsageMetric(),
        NNCapacityUsageMetric(),
        JournalTransactionInfoMetric(),
        DatanodeFSDatasetState(),
        corruptfilesMetric(),
        TopUserOpCountsMetric()
    )
    Runner.run(collector)