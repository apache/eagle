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

        for input in self.config["input"]:
            if not input.has_key("host"):
                input["host"] = socket.getfqdn()
            if input.has_key("component") and input["component"] == "namenode":
                hosts.append(input)

        if not bool(hosts):
            logging.warn("non hosts are configured as 'namenode' in 'input' config, exit")
            return

        logging.info("Checking namenode HA: " + str(hosts))

        active_count = 0
        standby_count = 0
        failed_count = 0

        failed_host_list = []
        host_name_list = []

        for host in hosts:
            try:
                if host.has_key("source_host"):
                    host["host"] = host["source_host"]

                host_name_list.append(host["host"])
                bean = JmxReader(host["host"], host["port"], host["https"]) \
                    .read_query("/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem&anonymous=true") \
                    .get_jmx_bean_by_name("Hadoop:service=NameNode,name=FSNamesystem")
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
                failed_host_list.append(host["host"])


        total_count = len(hosts)
        all_hosts_name = string.join(host_name_list,",")

        self.collect({
            "host": all_hosts_name,
            "component": "namenode",
            "metric": "hadoop.namenode.hastate.total.count",
            "value": total_count
        })

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

        if len(failed_host_list) > 0:
            all_hosts_name = string.join(failed_host_list,",")

        self.collect({
            "host": all_hosts_name,
            "component": "namenode",
            "metric": "hadoop.namenode.hastate.failed.count",
            "value": failed_count
        })

class HadoopHBaseHAChecker(MetricCollector):
    def run(self):
        hosts = []

        for input in self.config["input"]:
            if not input.has_key("host"):
                input["host"] = socket.getfqdn()
            if input.has_key("component") and input["component"] == "hbasemaster":
                hosts.append(input)

        if not bool(hosts):
            logging.warn("non hosts are configured as 'hbasemaster' in 'input' config, exit")
            return

        logging.info("Checking HBase HA: " + str(hosts))

        active_count = 0
        standby_count = 0
        failed_count = 0

        failed_host_list = []
        host_name_list = []

        for host in hosts:
            try:
                if host.has_key("source_host"):
                    host["host"] = host["source_host"]
                host_name_list.append(host["host"])
                bean = JmxReader(host["host"], host["port"], host["https"]) \
                    .read_query("/jmx?qry=Hadoop:service=HBase,name=Master,sub=Server&anonymous=true") \
                    .get_jmx_bean_by_name("Hadoop:service=HBase,name=Master,sub=Server")
                if not bean:
                    logging.error("JMX Bean[Hadoop:service=HBase,name=Master,sub=Server] is null from " + host["host"])
                if bean.has_key("tag.isActiveMaster"):
                    logging.debug(str(host) + " is " + bean["tag.isActiveMaster"])
                    if bean["tag.isActiveMaster"] == "true":
                        active_count += 1
                    else:
                        standby_count += 1
                else:
                    logging.info("'tag.isActiveMaster' not found from jmx of " + host["host"] + ":" + host["port"])
            except Exception as e:
                logging.exception("failed to read jmx from " + host["host"] + ":" + host["port"])
                failed_count += 1
                failed_host_list.append(host["host"])

        total_count = len(hosts)
        all_hosts_name = string.join(host_name_list,",")

        self.collect({
            "host": all_hosts_name,
            "component": "hbasemaster",
            "metric": "hadoop.hbasemaster.hastate.total.count",
            "value": total_count
        })

        self.collect({
            "host": all_hosts_name,
            "component": "hbasemaster",
            "metric": "hadoop.hbasemaster.hastate.active.count",
            "value": active_count
        })

        self.collect({
            "host": all_hosts_name,
            "component": "hbasemaster",
            "metric": "hadoop.hbasemaster.hastate.standby.count",
            "value": standby_count
        })

        if len(failed_host_list) > 0:
            all_hosts_name = string.join(failed_host_list,",")

        self.collect({
            "host": all_hosts_name,
            "component": "hbasemaster",
            "metric": "hadoop.hbasemaster.hastate.failed.count",
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

        active_count = 0
        standby_count = 0
        failed_count = 0

        failed_host_list = []

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
                failed_host_list.append(host["host"])

        total_count = len(hosts)
        all_hosts_name = string.join(all_hosts,",")

        self.collect({
            "host": all_hosts_name,
            "component": "resourcemanager",
            "metric": "hadoop.resourcemanager.hastate.total.count",
            "value": total_count
        })

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

        if len(failed_host_list) > 0:
            all_hosts_name = string.join(failed_host_list,",")

        self.collect({
            "host": all_hosts_name,
            "component": "resourcemanager",
            "metric": "hadoop.resourcemanager.hastate.failed.count",
            "value": failed_count
        })

if __name__ == '__main__':
    Runner.run(HadoopNNHAChecker(), HadoopHBaseHAChecker(), HadoopRMHAChecker())