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

from metric_collector import JmxMetricCollector,JmxMetricListener,Runner
import json

class NNSafeModeMetric(JmxMetricListener):
    def on_metric(self, metric):
        if metric["metric"] == "hadoop.namenode.fsnamesystemstate.fsstate":
            if metric["value"] == "safeMode":
                metric["value"] = 1
            else:
                metric["value"] = 0
            self.collector.collect(metric)


class NNHAMetric(JmxMetricListener):
    PREFIX = "hadoop.namenode.fsnamesystem"

    def on_bean(self, bean):
        if bean["name"] == "Hadoop:service=NameNode,name=FSNamesystem":
            if bean[u"tag.HAState"] == "active":
                self.collector.on_bean_kv(self.PREFIX, "hastate", 0)
            else:
                self.collector.on_bean_kv(self.PREFIX, "hastate", 1)


class MemortUsageMetric(JmxMetricListener):
    PREFIX = "hadoop.namenode.jvm"

    def on_bean(self, bean):
        if bean["name"] == "Hadoop:service=NameNode,name=JvmMetrics":
            memnonheapusedusage = round(float(bean['MemNonHeapUsedM']) / float(bean['MemNonHeapMaxM']) * 100.0, 2)
            self.collector.on_bean_kv(self.PREFIX, "memnonheapusedusage", memnonheapusedusage)
            memnonheapcommittedusage = round(float(bean['MemNonHeapCommittedM']) / float(bean['MemNonHeapMaxM']) * 100,
                                             2)
            self.collector.on_bean_kv(self.PREFIX, "memnonheapcommittedusage", memnonheapcommittedusage)
            memheapusedusage = round(float(bean['MemHeapUsedM']) / float(bean['MemHeapMaxM']) * 100, 2)
            self.collector.on_bean_kv(self.PREFIX, "memheapusedusage", memheapusedusage)
            memheapcommittedusage = round(float(bean['MemHeapCommittedM']) / float(bean['MemHeapMaxM']) * 100, 2)
            self.collector.on_bean_kv(self.PREFIX, "memheapcommittedusage", memheapcommittedusage)


class NNCapacityUsageMetric(JmxMetricListener):
    PREFIX = "hadoop.namenode.fsnamesystemstate"

    def on_bean(self, bean):
        if bean["name"] == "Hadoop:service=NameNode,name=FSNamesystemState":
            capacityusage = round(float(bean['CapacityUsed']) / float(bean['CapacityTotal']) * 100, 2)
            self.collector.on_bean_kv(self.PREFIX, "capacityusage", capacityusage)

class NNNodeUsageMetric(JmxMetricListener):
    PREFIX = "hadoop.namenode.nodeusage"

    def on_bean(self, bean):
        if bean["name"] == "Hadoop:service=NameNode,name=NameNodeInfo":
            nodeusagedic = json.loads(bean["NodeUsage"]);
            nodeusage_detail_dic = nodeusagedic["nodeUsage"];
            min = round( float(nodeusage_detail_dic["min"].strip('%')) ,2 );
            max = round( float(nodeusage_detail_dic["max"].strip('%')) ,2 );
            median = round( float(nodeusage_detail_dic["median"].strip('%')) ,2 );
            stddev = round( float(nodeusage_detail_dic["stdDev"].strip('%')) ,2 );
            self.collector.on_bean_kv(self.PREFIX, "min", min)
            self.collector.on_bean_kv(self.PREFIX, "max", max)
            self.collector.on_bean_kv(self.PREFIX, "median", median)
            self.collector.on_bean_kv(self.PREFIX, "stddev", stddev)


class JournalTransactionInfoMetric(JmxMetricListener):
    PREFIX = "hadoop.namenode.journaltransaction"

    def on_bean(self, bean):
        if bean.has_key("JournalTransactionInfo"):
            JournalTransactionInfo = json.loads(bean.get("JournalTransactionInfo"))
            LastAppliedOrWrittenTxId = float(JournalTransactionInfo.get("LastAppliedOrWrittenTxId"))
            MostRecentCheckpointTxId = float(JournalTransactionInfo.get("MostRecentCheckpointTxId"))
            self.collector.on_bean_kv(self.PREFIX, "LastAppliedOrWrittenTxId", LastAppliedOrWrittenTxId)
            self.collector.on_bean_kv(self.PREFIX, "MostRecentCheckpointTxId", MostRecentCheckpointTxId)


if __name__ == '__main__':
    collector = JmxMetricCollector()
    collector.register(
            NNSafeModeMetric(),
            NNHAMetric(),
            MemortUsageMetric(),
            JournalTransactionInfoMetric(),
            NNCapacityUsageMetric(),
            NNNodeUsageMetric()
    )
    Runner.run(collector)
