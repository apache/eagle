#!/usr/bin/python

import os
import re
import time
import json
import sys
import socket
import types
import re
import errno

# load kafka-python
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '', 'lib/six'))
import six

# load kafka-python
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '', 'lib/kafka-python'))
from kafka import KafkaClient, SimpleProducer, SimpleConsumer

from util_func import *

TOPIC = "cronus_sys_metrics"
DATA_TYPE = "system"

METRIC_NAME_EXCLUDE = re.compile(r"[\(|\)]")

DEBUG_KAFKA_HOST = []
PROD_KAFKA_HOST = []

PORT_MAP = {
    "60030": "regionserver",
    "50075": "datanode",
    "50070": "namenode",
    "60010": "master",
    "50030": "resourcemanager",
    "50060": "nodemanager",
    "8480": "journalnode"
}

def readFile(filename):
    f = open(filename, 'r')
    s = f.read()
    f.close()
    return s

def kafka_connect(host):
    print "Connecting to kafka " + str(host)
    # To send messages synchronously
    kafka = KafkaClient(host, timeout=58)
    producer = SimpleProducer(kafka, batch_send=True, batch_send_every_n=500, batch_send_every_t=30)
    return kafka, producer


def kafka_close(kafka, producer):
    if producer is not None:
        producer.stop()
    if kafka is not None:
        kafka.close()


def kafka_produce(producer, topic, kafka_json):
    # Note that the application is responsible for encoding messages to type str
    if producer != None :
        producer.send_messages(topic, kafka_json)
    else:
        print kafka_json


def addExtraMetric(producer, kafka_dict, metric, value, device, topic):
    kafka_dict["timestamp"] = int(round(time.time() * 1000))
    kafka_dict["metric"] = DATA_TYPE + "." + metric.lower()
    kafka_dict["value"] = str(value)
    kafka_dict["device"] = device
    kafka_json = json.dumps(kafka_dict)
    print(kafka_json)
    kafka_produce(producer, topic, kafka_json)


def getCPU(producer, kafka_dict, topic):
    cpu_info = os.popen('cat /proc/stat').readlines()
    demension = ["cpu", "user", "nice", "system", "idle", "wait", "irq", "softirq", "steal", "guest"]

    total_cpu = 0
    total_cpu_usage = 0
    cpu_stat_pre = None

    data_dir = "/tmp/eagle_cpu_stat_previous"
    if os.path.exists(data_dir):
        fd = open(data_dir, "r")
        cpu_stat_pre = fd.read()
        fd.close()

    for item in cpu_info:
        if re.match(r'^cpu\d+', item) is None:
            continue

        items = re.split("\s+", item.strip())
        demens = min(len(demension), len(items))
        # print items
        tuple = dict()
        for i in range(1, demens):
            # if not isNumber(items[i]):
            # continue

            tuple[demension[i]] = int(items[i])

            kafka_dict['timestamp'] = int(round(time.time() * 1000))
            kafka_dict['metric'] = DATA_TYPE + "." + 'cpu.' + demension[i]
            kafka_dict['device'] = items[0]
            kafka_dict['value'] = items[i]
            kafka_json = json.dumps(kafka_dict)
            #print kafka_json
            kafka_produce(producer, topic, kafka_json)

        per_cpu_usage = tuple["user"] + tuple["nice"] + tuple["system"] + tuple["wait"] + tuple["irq"] + tuple[
            "softirq"] + tuple["steal"] + tuple["guest"]
        per_cpu_total = tuple["user"] + tuple["nice"] + tuple["system"] + tuple["idle"] + tuple["wait"] + tuple["irq"] + \
                        tuple["softirq"] + tuple["steal"] + tuple["guest"]
        total_cpu += per_cpu_total
        total_cpu_usage += per_cpu_usage

        # system.cpu.usage
        kafka_dict['timestamp'] = int(round(time.time() * 1000))
        kafka_dict['metric'] = DATA_TYPE + "." + 'cpu.' + "perusage"
        kafka_dict['device'] = items[0]
        kafka_dict['value'] = str(round(per_cpu_usage * 100.0 / per_cpu_total, 2))
        kafka_json = json.dumps(kafka_dict)
        print kafka_json
        kafka_produce(producer, topic, kafka_json)

    cup_stat_current = str(total_cpu_usage) + " " + str(total_cpu)
    print cup_stat_current
    fd = open(data_dir, "w")
    fd.write(cup_stat_current)
    fd.close()

    pre_total_cpu_usage = 0
    pre_total_cpu = 0
    if cpu_stat_pre != None:
        result = re.split("\s+", cpu_stat_pre.rstrip())
        pre_total_cpu_usage = int(result[0])
        pre_total_cpu = int(result[1])
    kafka_dict['timestamp'] = int(round(time.time() * 1000))
    kafka_dict['metric'] = DATA_TYPE + "." + 'cpu.' + "totalusage"
    kafka_dict['device'] = "cpu"
    kafka_dict['value'] = str(round((total_cpu_usage-pre_total_cpu_usage) * 100.0 / (total_cpu-pre_total_cpu), 2))
    kafka_json = json.dumps(kafka_dict)

    print kafka_json
    kafka_produce(producer, topic, kafka_json)


def getUptime(producer, kafka_dict, topic):
    demension = ["uptime.day", "idletime.day"]
    output = os.popen('cat /proc/uptime').readlines()

    for item in output:
        items = re.split("\s+", item.rstrip())
        for i in range(len(demension)):
            kafka_dict["timestamp"] = int(round(time.time() * 1000))
            kafka_dict["metric"] = DATA_TYPE + "." + 'uptime' + '.' + demension[i]
            kafka_dict["value"] = str(round(float(items[i]) / 86400, 2))
            kafka_json = json.dumps(kafka_dict)
            print kafka_json
            kafka_produce(producer, topic, kafka_json)


def getMemInfo(producer, kafka_dict, topic):
    output = os.popen('cat /proc/meminfo').readlines()
    mem_info = dict()
    for item in output:
        items = re.split(":?\s+", item.rstrip())
        # print items
        mem_info[items[0]] = int(items[1])
        itemNum = len(items)
        metric = 'memory' + '.' + items[0]
        if (len(items) > 2 ):
            metric = metric + '.' + items[2]
        kafka_dict["timestamp"] = int(round(time.time() * 1000))
        kafka_dict["metric"] = METRIC_NAME_EXCLUDE.sub("", DATA_TYPE + "." + metric.lower())
        kafka_dict["value"] = items[1]
        kafka_dict["device"] = 'memory'
        kafka_json = json.dumps(kafka_dict)
        print kafka_json
        kafka_produce(producer, topic, kafka_json)
    usage = (mem_info['MemTotal'] - mem_info['MemFree'] - mem_info['Buffers'] - mem_info['Cached']) * 100.0 / mem_info[
        'MemTotal']
    usage = round(usage, 2)
    addExtraMetric(producer, kafka_dict, "memory.usage", usage, "memory", topic)


def getLoadAvg(producer, kafka_dict, topic):
    demension = ['cpu.loadavg.1min', 'cpu.loadavg.5min', 'cpu.loadavg.15min']
    output = os.popen('cat /proc/loadavg').readlines()
    for item in output:
        items = re.split("\s+", item.rstrip())

        demens = min(len(demension), len(items))
        for i in range(demens):
            kafka_dict["timestamp"] = int(round(time.time() * 1000))
            kafka_dict["metric"] = DATA_TYPE + "." + demension[i]
            kafka_dict["value"] = items[i]
            kafka_dict["device"] = 'cpu'
            kafka_json = json.dumps(kafka_dict)
            print kafka_json
            kafka_produce(producer, topic, kafka_json)


def getIpmiCPUTemp(producer, kafka_dict, topic):
    output = os.popen('sudo ipmitool sdr | grep Temp | grep CPU').readlines()
    for item in output:
        items = re.split("^(CPU\d+)\sTemp\.\s+\|\s+(\d+|\d+\.\d+)\s", item.rstrip())
        kafka_dict["timestamp"] = int(round(time.time() * 1000))
        kafka_dict["metric"] = DATA_TYPE + "." + 'cpu.temp'
        kafka_dict["value"] = items[2]
        kafka_dict["device"] = item[1]
        kafka_json = json.dumps(kafka_dict)
        print kafka_json
        kafka_produce(producer, topic, kafka_json)


def getInterface(producer, kafka_dict, topic):
    demension = ['receivedbytes', 'receivedpackets', 'receivederrs', 'receiveddrop', 'transmitbytes', 'transmitpackets',
                 'transmiterrs', 'transmitdrop']
    output = os.popen("cat /proc/net/dev").readlines()

    for item in output:
        if re.match(r'^\s+eth\d+:', item) is None:
            continue
        items = re.split("[:\s]+", item.strip())
        filtered_items = items[1:5] + items[9:13]

        for i in range(len(demension)):
            kafka_dict["timestamp"] = int(round(time.time() * 1000))
            kafka_dict['metric'] = DATA_TYPE + "." + 'nic.' + demension[i]
            kafka_dict["value"] = filtered_items[i]
            kafka_dict["device"] = items[0]
            kafka_json = json.dumps(kafka_dict)
            print kafka_json
            kafka_produce(producer, topic, kafka_json)


def getSmartDisk(producer, kafka_dict, topic):
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
            kafka_dict['metric'] = DATA_TYPE + "." + metric.lower()
            kafka_dict["timestamp"] = int(round(time.time() * 1000))
            kafka_dict["value"] = lineitems[-1]
            kafka_dict["device"] = 'smartdisk'
            kafka_json = json.dumps(kafka_dict)
            print kafka_json
            kafka_produce(producer, topic, kafka_json)


def getDiskStat(producer, kafka_dict, topic):
    demension = ['readrate', 'writerate', 'avgwaittime', 'utilization', 'disktotal', 'diskused', 'usage']
    iostat_output = os.popen("iostat -xk 1 2 | grep ^sd").readlines()
    # remove the first set of elements
    iostat_output = iostat_output[len(iostat_output) / 2:]
    # print iostat_output
    iostat_dict = {}
    for item in iostat_output:
        items = re.split('\s+', item.strip())
        # print items
        filtered_items = [items[5], items[6], items[9], items[11]]
        iostat_dict[items[0]] = filtered_items
    # print iostat_dict

    disk_output = os.popen("df -k | grep ^/dev").readlines()
    for item in disk_output:
        items = re.split('\s+', item.strip())
        fs = re.split('^\/dev\/(\w+)\d+$', items[0])
        disk = fs[1]
        iostat_dict[disk].append(items[1])
        iostat_dict[disk].append(items[2])
        iostat_dict[disk].append(items[4].rstrip('%'))
    #print iostat_dict

    for key, metrics in iostat_dict.iteritems():
        for i in range(len(metrics)):
            metric = 'disk.' + demension[i]
            kafka_dict['metric'] = DATA_TYPE + "." + metric.lower()
            kafka_dict["timestamp"] = int(round(time.time() * 1000))
            kafka_dict["value"] = metrics[i]
            kafka_dict["device"] = key
            kafka_json = json.dumps(kafka_dict)
            # print kafka_json
            kafka_produce(producer, topic, kafka_json)


def get_services(host):
    service_list = list()
    socket.setdefaulttimeout(1)
    for (key, value) in PORT_MAP.items():
        try:
            handle = None
            port = int(key)
            handle = socket.socket().connect((host, port))
            service_list.append(value)
        except socket.error as err:
            # if err.errno != errno.ECONNREFUSED:
            # service_list.append(value)
            pass
        finally:
            if handle != None:
                handle.close()

    return service_list

def tryGetSystemMetric(type, func, *args):
    try:
        func(*args)
    except:
        print type + " does not work, ignore"

DEVICE_CONF = {
    "cpustat": getCPU,
    "uptime": getUptime,
    "meminfo": getMemInfo,
    "loadavg": getLoadAvg,
    "ipmicputemp": getIpmiCPUTemp,
    "network": getInterface,
    "smartdisk": getSmartDisk,
    "diskstat": getDiskStat
}

def main(argv):
    kafka = None
    producer = None
    topic = None
    try:
        # read the kafka.ini
        config = loadConfigFile('eagle-collector.conf')
        print config

        site = config[u'env'].get('site').encode('utf-8')
        component = config[u'input'].get('component').encode('utf-8')
        host = socket.getfqdn()
        print host

        outputs = [s.encode('utf-8') for s in config[u'output']]

        if('kafka' in outputs):
            kafkaConfig = config[u'output'].get(u'kafka')
            brokerList = kafkaConfig.get('brokerList')
            topic = kafkaConfig.get('topic')
            kafka, producer = kafka_connect(brokerList)

        kafka_dict = {"host": host, "value": 0, "device": ''}
        services = get_services(host)
        print services
        for service in services:
            kafka_dict[service] = 'true'

        for type, func in DEVICE_CONF.items():
            print type + ":" + str(func)
            tryGetSystemMetric(type, func, kafka, kafka_dict, topic)

    except Exception, e:
        print 'main except: ', e

    finally:
        kafka_close(kafka, producer)
        return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
