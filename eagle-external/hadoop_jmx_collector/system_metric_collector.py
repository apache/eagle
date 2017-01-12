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

from metric_collector import MetricCollector, Runner
import logging, socket, string, os, re, time


class SystemMetricCollector(MetricCollector):
    METRIC_PREFIX = "system"
    METRIC_NAME_EXCLUDE = re.compile(r"[\(|\)]")

    def run(self):
        if self.config["env"].has_key("cpu_stat_file"):
            self.cpu_stat_file = self.config["env"]["cpu_stat_file"]
            logging.info("Overrode env.cpu_stat_file: %s", self.cpu_stat_file)
        else:
            self.cpu_stat_file = "/tmp/eagle_cpu_usage_state"
            logging.info("Using default env.cpu_stat_file: %s", self.cpu_stat_file)

        self.try_exec_func(
            self.collect_cpu_metric,
            self.collect_uptime_metric,
            self.collect_memory_metric,
            self.collect_loadavg_metric,
            self.collect_cpu_temp_metric,
            self.collect_nic_metric,
            self.collect_smartdisk_metric,
            self.collect_diskstat_metric
        )

    def try_exec_func(self, *funcs):
        result = dict()
        succeed_num = 0
        failed_num = 0
        for func in funcs:
            try:
                logging.info("Executing: %s", func.__name__)
                func()
                result[func.__name__] = "success"
                succeed_num = succeed_num + 1
            except Exception as e:
                logging.warn("Failed to execute: %s", func.__name__)
                logging.exception(e)
                result[func.__name__] = "error: %s: %s" % (type(e), e)
                failed_num = failed_num + 1
        result_desc = ""
        for key in result:
            result_desc = result_desc + "%-30s: %-30s\n" % (key, result[key])
        logging.info("Execution result (total: %s, succeed: %s, failed: %s): \n\n%s", len(funcs), succeed_num,
                     failed_num, result_desc)

    # ====================================
    # CPU Usage
    # ====================================

    def collect_cpu_metric(self):
        """
        CPU Usage Percentage Metrics:

        system.cpu.usage: (user + nice + system + wait + irq + softirq + steal + guest) / (user + nice + system + idle + wait + irq + softirq + steal + guest)

            Example:

                {'timestamp': 1483594861458, 'metric': 'system.cpu.usage', 'site': u'sandbox', 'value': 0.048, 'host': 'localhost', 'device': 'cpuN'}

        system.cpu.totalusage: Sum(Each CPU Usage) / Sum (CPU Total)

            Example:

                {'timestamp': 1483594861484, 'metric': 'system.cpu.totalusage', 'site': u'sandbox', 'value': 0.17, 'host': 'sandbox.hortonworks.com', 'device': 'cpu'}

        """

        cpu_metric = self.new_metric()
        cpu_info = os.popen('cat /proc/stat').readlines()
        dimensions = ["cpu", "user", "nice", "system", "idle", "wait", "irq", "softirq", "steal", "guest"]

        total_cpu = 0
        total_cpu_usage = 0
        cpu_stat_pre = None

        data_dir = self.cpu_stat_file
        if os.path.exists(data_dir):
            fd = open(data_dir, "r")
            cpu_stat_pre = fd.read()
            fd.close()

        for item in cpu_info:
            if re.match(r'^cpu\d+', item) is None:
                continue

            items = re.split("\s+", item.strip())
            demens = min(len(dimensions), len(items))
            metric_event = dict()
            for i in range(1, demens):
                metric_event[dimensions[i]] = int(items[i])
                cpu_metric['timestamp'] = int(round(time.time() * 1000))
                cpu_metric['metric'] = self.METRIC_PREFIX + "." + 'cpu.' + dimensions[i]
                cpu_metric['device'] = items[0]
                cpu_metric['value'] = items[i]
                self.collect(cpu_metric)

            per_cpu_usage = metric_event["user"] + metric_event["nice"] + metric_event["system"] + metric_event[
                "wait"] + metric_event["irq"] + metric_event["softirq"] + metric_event["steal"] + metric_event["guest"]
            per_cpu_total = metric_event["user"] + metric_event["nice"] + metric_event["system"] + metric_event[
                "idle"] + metric_event["wait"] + metric_event["irq"] + metric_event["softirq"] + metric_event["steal"] + metric_event["guest"]
            total_cpu += per_cpu_total
            total_cpu_usage += per_cpu_usage

            # system.cpu.usage
            cpu_metric['timestamp'] = int(round(time.time() * 1000))
            cpu_metric['metric'] = self.METRIC_PREFIX + "." + 'cpu.' + "usage"
            cpu_metric['device'] = items[0]
            cpu_metric['value'] = per_cpu_usage * 1.0 /per_cpu_total
            self.collect(cpu_metric)

        cup_stat_current = str(total_cpu_usage) + " " + str(total_cpu)
        logging.info("Current cpu stat: %s", cup_stat_current)
        fd = open(data_dir, "w")
        fd.write(cup_stat_current)
        fd.close()

        pre_total_cpu_usage = 0
        pre_total_cpu = 0
        if cpu_stat_pre != None:
            result = re.split("\s+", cpu_stat_pre.rstrip())
            pre_total_cpu_usage = int(result[0])
            pre_total_cpu = int(result[1])
        cpu_metric['timestamp'] = int(round(time.time() * 1000))
        cpu_metric['metric'] = self.METRIC_PREFIX + "." + 'cpu.' + "totalusage"
        cpu_metric['device'] = "cpu"
        cpu_metric['value'] = (total_cpu_usage - pre_total_cpu_usage) * 1.0 / (total_cpu - pre_total_cpu)

        self.collect(cpu_metric)

    # ====================================
    # OS Up Time
    # ====================================

    def collect_uptime_metric(self):
        metric = self.new_metric()
        demension = ["uptime.day", "idletime.day"]
        output = os.popen('cat /proc/uptime').readlines()

        for item in output:
            items = re.split("\s+", item.rstrip())
            for i in range(len(demension)):
                metric["timestamp"] = int(round(time.time() * 1000))
                metric["metric"] = self.METRIC_PREFIX + "." + 'uptime' + '.' + demension[i]
                metric["value"] = str(round(float(items[i]) / 86400, 2))
                self.collect(metric)

    # ====================================
    # Memory
    # ====================================

    def collect_memory_metric(self):
        event = self.new_metric()
        event["host"] = self.fqdn
        output = os.popen('cat /proc/meminfo').readlines()
        mem_info = dict()
        for item in output:
            items = re.split(":?\s+", item.rstrip())
            # print items
            mem_info[items[0]] = int(items[1])
            itemNum = len(items)
            metric = 'memory' + '.' + items[0]
            if (len(items) > 2):
                metric = metric + '.' + items[2]
            event["timestamp"] = int(round(time.time() * 1000))
            event["metric"] = self.METRIC_NAME_EXCLUDE.sub("", self.METRIC_PREFIX + "." + metric.lower())
            event["value"] = items[1]
            event["device"] = 'memory'
            self.collect(event)

        usage = (mem_info['MemTotal'] - mem_info['MemFree'] - mem_info['Buffers'] - mem_info['Cached']) * 100.0 / \
                mem_info[
                    'MemTotal']
        usage = round(usage, 2)
        self.emit_metric(event, self.METRIC_PREFIX, "memory.usage", usage, "memory")

    # ====================================
    # Load AVG
    # ====================================

    def collect_loadavg_metric(self):
        """
        Collect Load Avg Metrics
        """
        demension = ['cpu.loadavg.1min', 'cpu.loadavg.5min', 'cpu.loadavg.15min']
        output = os.popen('cat /proc/loadavg').readlines()
        for item in output:
            items = re.split("\s+", item.rstrip())
            demens = min(len(demension), len(items))
            for i in range(demens):
                event = self.new_metric()
                event["timestamp"] = int(round(time.time() * 1000))
                event["metric"] = self.METRIC_PREFIX + "." + demension[i]
                event["value"] = items[i]
                event["device"] = 'cpu'
                self.collect(event)

    # ====================================
    # IPMI CPU Temp
    # ====================================

    def collect_cpu_temp_metric(self):
        output = os.popen('sudo ipmitool sdr | grep Temp | grep CPU').readlines()
        for item in output:
            items = re.split("^(CPU\d+)\sTemp\.\s+\|\s+(\d+|\d+\.\d+)\s", item.rstrip())
            event = self.new_metric()
            event["timestamp"] = int(round(time.time() * 1000))
            event["metric"] = DATA_TYPE + "." + 'cpu.temp'
            event["value"] = items[2]
            event["device"] = item[1]
            self.collect(event)

    # ====================================
    # NIC Metrics
    # ====================================

    def collect_nic_metric(self):
        demension = ['receivedbytes', 'receivedpackets', 'receivederrs', 'receiveddrop', 'transmitbytes',
                     'transmitpackets',
                     'transmiterrs', 'transmitdrop']
        output = os.popen("cat /proc/net/dev").readlines()

        for item in output:
            if re.match(r'^\s+eth\d+:', item) is None:
                continue
            items = re.split("[:\s]+", item.strip())
            filtered_items = items[1:5] + items[9:13]

            for i in range(len(demension)):
                kafka_dict = self.new_metric()
                kafka_dict["timestamp"] = int(round(time.time() * 1000))
                kafka_dict['metric'] = self.METRIC_PREFIX + "." + 'nic.' + demension[i]
                kafka_dict["value"] = filtered_items[i]
                kafka_dict["device"] = items[0]
                self.collect(kafka_dict)

    # ====================================
    # Smart Disk Metrics
    # ====================================

    def collect_smartdisk_metric(self):
        harddisks = os.popen("lsscsi").readlines()
        for item in harddisks:
            items = re.split('\/', item.strip())
            # print items
            smartctl = os.popen('sudo smartctl -A /dev/' + items[-1]).readlines()
            for line in smartctl:
                line = line.strip()
                if re.match(r'^[\d]+\s', line) is None:
                    continue
                lineitems = re.split("\s+", line)
                metric = 'smartdisk.' + lineitems[1]
                kafka_dict = self.new_metric()
                kafka_dict['metric'] = DATA_TYPE + "." + metric.lower()
                kafka_dict["timestamp"] = int(round(time.time() * 1000))
                kafka_dict["value"] = lineitems[-1]
                kafka_dict["device"] = 'smartdisk'
                self.collect(kafka_dict)

    # ====================================
    # Disk Stat Metrics
    # ====================================

    def collect_diskstat_metric(self):
        """
        FIXME: IndexError: list index out of range
        """
        demension = ['readrate', 'writerate', 'avgwaittime', 'utilization', 'disktotal', 'diskused', 'usage']
        iostat_output = os.popen("iostat -xk 1 2 | grep ^sd").readlines()
        # remove the first set of elements
        iostat_output = iostat_output[len(iostat_output) / 2:]
        iostat_dict = {}
        for item in iostat_output:
            items = re.split('\s+', item.strip())
            filtered_items = [items[5], items[6], items[9], items[11]]
            iostat_dict[items[0]] = filtered_items

        disk_output = os.popen("df -k | grep ^/dev").readlines()
        for item in disk_output:
            items = re.split('\s+', item.strip())
            disks = re.split('^\/dev\/(\w+)\d+$', items[0])
            logging.info(len(disks))
            disk = disks[1]
            iostat_dict[disk].append(items[1])
            iostat_dict[disk].append(items[2])
            iostat_dict[disk].append(items[4].rstrip('%'))

        for key, metrics in iostat_dict.iteritems():
            for i in range(len(metrics)):
                metric = 'disk.' + demension[i]
                kafka_dict = self.new_metric()
                kafka_dict['metric'] = DATA_TYPE + "." + metric.lower()
                kafka_dict["timestamp"] = int(round(time.time() * 1000))
                kafka_dict["value"] = metrics[i]
                kafka_dict["device"] = key
                self.collect(kafka_dict)

    # ====================================
    # Helper Methods
    # ====================================

    def emit_metric(self, event, prefix, metric, value, device):
        event["timestamp"] = int(round(time.time() * 1000))
        event["metric"] = prefix + "." + metric.lower()
        event["value"] = str(value)
        event["device"] = device
        self.collect(event)

    def new_metric(self):
        metric = dict()
        metric["host"] = self.fqdn
        return metric


if __name__ == '__main__':
    Runner.run(SystemMetricCollector())
