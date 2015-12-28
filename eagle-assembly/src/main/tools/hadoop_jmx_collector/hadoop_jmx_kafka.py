#!/usr/bin/python
# Parse jmxjson output from hadoop/hbase component
# Author: Qingwen Zhao, qingwzhao@ebay.com, Hadoop Engineering Team, eBay Inc.
# Python version: 2.7


import os
import re
import time
import json
import urllib2
import sys
import socket
import types
import httplib

# load six
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '', 'lib/six'))
import six

# load kafka-python
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '', 'lib/kafka-python'))
from kafka import KafkaClient, SimpleProducer, SimpleConsumer


DATA_TYPE = "hadoop"

def isNumber(str):
    try:
        if str == None or isinstance(str, (bool)):
            return False
        float(str)
        return True
    except:
        return False


def getMetricName(propertyList, context):
    name_index = [i[0] for i in propertyList].index('name')
    propertyList[name_index][1] = context
    return '.'.join([i[1] for i in propertyList])


def readFile(filename):
    f = open(filename, 'r')
    s = f.read()
    f.close()
    return s


def loadConfigFile(filename):
    # read the self-defined filters

    script_dir = os.path.dirname(__file__)
    rel_path = "./" + filename
    abs_file_path = os.path.join(script_dir, rel_path)
    json_file = readFile(abs_file_path)
    #print json_file

    try:
        config = json.loads(json_file)

    except ValueError:
        print "cronus.ini error loading"
    return config


def readUrl(url, https=False):
    jmxjson = 'error'
    try:
        if https:
            print "Reading https://" + str(url) + "/jmx?anonymous=true&qry=Hadoop:*"
            c = httplib.HTTPSConnection(url, timeout=57)
            c.request("GET", "/jmx?anonymous=true&qry=Hadoop:*")
            response = c.getresponse()
        else:
            print "Reading http://" + str(url) + "/jmx?anonymous=true&qry=Hadoop:*"
            response = urllib2.urlopen("http://" + url + '/jmx?anonymous=true&qry=Hadoop:*', timeout=57)
    except Exception, e:
        print 'Reason: ', e
    else:
        # everything is fine
        jmxjson = response.read()
        response.close()
    finally:
        return jmxjson


def addExtraMetric(bean, kafka_dict, metric_prefix_name, producer, topic):
    PercentVal = None
    PercentVal = round(float(bean['MemNonHeapUsedM']) / float(bean['MemNonHeapMaxM']) * 100.0, 2)
    send_output_message(producer, topic, kafka_dict, metric_prefix_name + ".MemNonHeapUsedUsage", PercentVal)
    PercentVal = round(float(bean['MemNonHeapCommittedM']) / float(bean['MemNonHeapMaxM']) * 100, 2)
    send_output_message(producer, topic, kafka_dict, metric_prefix_name + ".MemNonHeapCommittedUsage", PercentVal)
    PercentVal = round(float(bean['MemHeapUsedM']) / float(bean['MemHeapMaxM']) * 100, 2)
    send_output_message(producer, topic, kafka_dict, metric_prefix_name + ".MemHeapUsedUsage", PercentVal)
    PercentVal = round(float(bean['MemHeapCommittedM']) / float(bean['MemHeapMaxM']) * 100, 2)
    send_output_message(producer, topic, kafka_dict, metric_prefix_name + ".MemHeapCommittedUsage", PercentVal)


def send_output_message(producer, topic, kafka_dict, metric, value):
    kafka_dict["timestamp"] = int(round(time.time() * 1000))
    kafka_dict["metric"] = DATA_TYPE + "." + metric.lower()
    kafka_dict["value"] = str(value)
    kafka_json = json.dumps(kafka_dict)

    if producer != None:
        producer.send_messages(topic, kafka_json)
    else:
        print(kafka_json)


def getHadoopData(producer, topic, config, beans, dataMap):
    selected_group = [s.encode('utf-8') for s in config[u'filter'].get('monitoring.group.selected')]
    #print selected_group

    for bean in beans:
        kafka_dict = dataMap.copy()
        mbean = bean[u'name']  # mbean is of the form "domain:key=value,...,foo=bar"
        mbean_domain, mbean_properties = mbean.rstrip().split(":", 1)
        mbean_domain = mbean_domain.lower()

        # print mbean_domain
        if mbean_domain not in selected_group:
            # print "Unexpected mbean domain = %s on %s" % (mbean_domain, mbean)
            continue

        mbean_list = list(prop.split("=", 1)
                          for prop in mbean_properties.split(","))
        mbean_properties = dict((i[0], i[1]) for i in mbean_list)

        # kafka_dict.update(mbean_properties)
        kafka_dict['type'] = mbean_properties['service'].lower()

        # metric prefix name
        context = bean.get("tag.Context", "")
        if context == "":
            metric_prefix_name = '.'.join([i[1] for i in mbean_list])
        else:
            metric_prefix_name = getMetricName(mbean_list, context)

        ## !HARD CODE to add some metrics
        if mbean_properties['name'].lower() == 'jvmmetrics':
            addExtraMetric(bean, kafka_dict, metric_prefix_name, producer, topic)

        # print kafka_dict
        for key, value in bean.iteritems():
            #print key, value
            key = key.lower()
            if not isNumber(value) or re.match(r'tag.*', key):
                continue

            if mbean_domain == 'hadoop' and re.match(r'^namespace', key):
                #print key
                items = re.split('_table_', key)
                key = items[1]
                items = re.split('_region_', key)
                kafka_dict['table'] = items[0]
                items = re.split('_metric_', items[1])
                kafka_dict['region'] = items[0]
                key = items[1]

            metric = metric_prefix_name + '.' + key
            send_output_message(producer, topic, kafka_dict, metric, value)


def kafka_connect(host):
    # To send messages synchronously
    print "Connecting kafka: " + str(host)
    kafka = KafkaClient(host, timeout=58)
    producer = SimpleProducer(kafka, batch_send=True, batch_send_every_n=500, batch_send_every_t=30)
    return kafka, producer


def kafka_close(kafka, producer):
    if producer is not None:
        producer.stop()
    if kafka is not None:
        kafka.close()


def loadJmxData(host, inputConfig):
    port = inputConfig.get('port')
    https = inputConfig.get('https')

    url = host + ':' + port
    #print url

    jmxjson = readUrl(url, https)
    #jmxjson = readFile("jmx")

    if jmxjson == 'error':
        print 'jmx load error'

    # transfer the json string into dict
    jmx = json.loads(jmxjson)
    beans = jmx[u'beans']

    return beans


def main():
    kafka = None
    producer = None
    topic = None

    try:
        #start = time.clock()

        # read the kafka.ini
        config = loadConfigFile('eagle-collector.conf')
        #print config

        cluster = config[u'env'].get('cluster').encode('utf-8')
        datacenter = config[u'env'].get('datacenter').encode('utf-8')
        site = config[u'env'].get('site').encode('utf-8')
        host = socket.getfqdn()

        beans = loadJmxData(host, config[u'input'])

        outputs = [s.encode('utf-8') for s in config[u'output']]
        #print outputs

        if('kafka' in outputs):
            kafkaConfig = config[u'output'].get(u'kafka')
            brokerList = kafkaConfig.get('brokerList')
            topic = kafkaConfig.get('topic')
            #print brokerList
            kafka, producer = kafka_connect(brokerList)

        dataMap = {"host": host, "timestamp": '', "metric": '', "value": ''}

        getHadoopData(producer, topic, config, beans, dataMap)

    except Exception, e:
        print 'main except: ', e

    finally:
        if kafka != None and producer != None:
            kafka_close(kafka, producer)

        #elapsed = (time.clock() - start)
        #print("Time used:",elapsed)


if __name__ == "__main__":
    main()


