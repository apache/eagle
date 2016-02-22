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


# !/usr/bin/python

from base import MetricCollector, JmxReader
import logging


class HadoopNNHAChecker(MetricCollector):
    def run(self):
        name_node_config = self.config["env"]["name_node"]
        hosts = name_node_config["hosts"]
        port = name_node_config["port"]
        https = name_node_config["https"]

        total_count = len(hosts)

        self.collect({
            "host": ",".join(hosts),
            "component": "namenode",
            "metric": "hadoop.namenode.hastate.total.count",
            "value": total_count
        })
        active_count = 0
        standby_count = 0

        for host in hosts:
            bean = JmxReader(host, port, https).get_jmx_bean_by_name("Hadoop:service=NameNode,name=FSNamesystem")
            logging.debug(host + " is " + bean["tag.HAState"])
            if bean["tag.HAState"] == "active":
                active_count += 1
            else:
                standby_count += 1

        self.collect({
            "host": ",".join(hosts),
            "component": "namenode",
            "metric": "hadoop.namenode.hastate.active.count",
            "value": active_count
        })
        self.collect({
            "host": ",".join(hosts),
            "component": "namenode",
            "metric": "hadoop.namenode.hastate.standby.count",
            "value": standby_count
        })


if __name__ == '__main__':
    HadoopNNHAChecker().start()
