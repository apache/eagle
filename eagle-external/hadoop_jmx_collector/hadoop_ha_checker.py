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

from metric_collector import MetricCollector, JmxReader, YarnWSReader, Runner
import logging,socket,string

class HadoopNNHAChecker(MetricCollector):
    def run(self):
        hosts = []
        host_name_list = []
        for input in self.config["input"]:
            if not input.has_key("host"):
                input["host"] = socket.getfqdn()
            if input.has_key("component") and input["component"] == "namenode":
                hosts.append(input)
                host_name_list.append(input["host"])

        if not bool(hosts):
            logging.warn("non hosts are configured as 'namenode' in 'input' config, exit")
            return

        logging.info("Checking namenode HA: " + str(hosts))
        total_count = len(hosts)

        all_hosts_name = string.join(host_name_list,",")

        self.collect({
            "host": all_hosts_name,
            "component": "namenode",
            "metric": "hadoop.namenode.hastate.total.count",
            "value": total_count
        })

        active_count = 0
        standby_count = 0
        failed_count = 0

        for host in hosts:
            try:
                bean = JmxReader(host["host"], host["port"], host["https"]).open().get_jmx_bean_by_name(
                        "Hadoop:service=NameNode,name=FSNamesystem")
                if not bean:
                    logging.error("JMX Bean[Hadoop:service=NameNode,name=FSNamesystem] is null from " + host["host"])
                if bean.has_key("tag.HAState"):
                    logging.debug(str(host) + " is " + bean["tag.HAState"])
                    if bean["tag.HAState"] == "active":
                        active_count += 1
                    else:
                        standby_count += 1
                else:
                    logging.info("'tag.HAState' not found from jmx of " + host["host"] + ":" + host["port"])
            except Exception as e:
                logging.exception("failed to read jmx from " + host["host"] + ":" + host["port"])
                failed_count += 1
        self.collect({
            "host": all_hosts_name,
            "component": "namenode",
            "metric": "hadoop.namenode.hastate.active.count",
            "value": active_count
        })

        self.collect({
            "host": all_hosts_name,
            "component": "namenode",
            "metric": "hadoop.namenode.hastate.standby.count",
            "value": standby_count
        })

        self.collect({
            "host": all_hosts_name,
            "component": "namenode",
            "metric": "hadoop.namenode.hastate.failed.count",
            "value": failed_count
        })

class HadoopRMHAChecker(MetricCollector):
    def run(self):
        hosts = []
        all_hosts = []
        for input in self.config["input"]:
            if not input.has_key("host"):
                input["host"] = socket.getfqdn()
            if input.has_key("component") and input["component"] == "resourcemanager":
                hosts.append(input)
                all_hosts.append(input["host"])
        if not bool(hosts):
            logging.warn("Non hosts are configured as 'resourcemanager' in 'input' config, exit")
            return

        logging.info("Checking resource manager HA: " + str(hosts))
        total_count = len(hosts)
        all_hosts_name = string.join(all_hosts,",")

        self.collect({
            "host": all_hosts_name,
            "component": "resourcemanager",
            "metric": "hadoop.resourcemanager.hastate.total.count",
            "value": total_count
        })

        active_count = 0
        standby_count = 0
        failed_count = 0

        for host in hosts:
            try:
                cluster_info = YarnWSReader(host["host"], host["port"], host["https"]).read_cluster_info()
                if not cluster_info:
                    logging.error("Cluster info is null from web service of " + host["host"])
                    raise Exception("cluster info is null from " + host["host"])
                if cluster_info["clusterInfo"]["haState"] == "ACTIVE":
                    active_count += 1
                else:
                    standby_count += 1
            except Exception as e:
                logging.error("Failed to read yarn ws from " + str(host))
                failed_count += 1

        self.collect({
            "host": all_hosts_name,
            "component": "resourcemanager",
            "metric": "hadoop.resourcemanager.hastate.active.count",
            "value": active_count
        })

        self.collect({
            "host": all_hosts_name,
            "component": "resourcemanager",
            "metric": "hadoop.resourcemanager.hastate.standby.count",
            "value": standby_count
        })

        self.collect({
            "host": all_hosts_name,
            "component": "resourcemanager",
            "metric": "hadoop.resourcemanager.hastate.failed.count",
            "value": failed_count
        })

if __name__ == '__main__':
    Runner.run(HadoopNNHAChecker(), HadoopRMHAChecker())