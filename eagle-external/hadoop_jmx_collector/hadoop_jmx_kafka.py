#!/usr/bin/python

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

from util_func import *
from metric_extensions import *


DATA_TYPE = "hadoop"

def readUrl(url, https=False):
    jmxjson = 'error'
    try:
        if https:
            print "Reading https://" + str(url) + "/jmx?anonymous=true"
            c = httplib.HTTPSConnection(url, timeout=57)
            c.request("GET", "/jmx?anonymous=true")
            response = c.getresponse()
        else:
            print "Reading http://" + str(url) + "/jmx?anonymous=true"
            response = urllib2.urlopen("http://" + url + '/jmx?anonymous=true', timeout=57)
    except Exception, e:
        print 'Reason: ', e
    else:
        # everything is fine
        jmxjson = response.read()
        response.close()
    finally:
        return jmxjson


def get_metric_prefix_name(mbean_attribute, context):
    mbean_list = list(prop.split("=", 1)
                      for prop in mbean_attribute.split(","))
    metric_prefix_name = None
    if context == "":
        metric_prefix_name = '.'.join([i[1] for i in mbean_list])
    else:
        name_index = [i[0] for i in mbean_list].index('name')
        mbean_list[name_index][1] = context
        metric_prefix_name = '.'.join([i[1] for i in mbean_list])
    return DATA_TYPE + "." + metric_prefix_name


def parse_hadoop_jmx(producer, topic, config, beans, dataMap, fat_bean):
    selected_group = [s.encode('utf-8') for s in config[u'filter'].get('monitoring.group.selected')]
    #print selected_group

    for bean in beans:
        kafka_dict = dataMap.copy()

        # mbean is of the form "domain:key=value,...,foo=bar"
        mbean = bean[u'name']
        mbean_domain, mbean_attribute = mbean.rstrip().split(":", 1)
        mbean_domain = mbean_domain.lower()

        # print mbean_domain
        if mbean_domain not in selected_group:
            # print "Unexpected mbean domain = %s on %s" % (mbean_domain, mbean)
            continue
        fat_bean.update(bean)
        context = bean.get("tag.Context", "")
        metric_prefix_name = get_metric_prefix_name(mbean_attribute, context)

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


def get_jmx_beans(host, port, https):
    # port = inputConfig.get('port')
    # https = inputConfig.get('https')
    url = host + ':' + port
    #print url

    jmxjson = readUrl(url, https)

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
        if (len(sys.argv) > 1):
            config = load_config(sys.argv[1])
        else:
            config = load_config('config.json')
        #print config

        site = config[u'env'].get('site').encode('utf-8')
        component = config[u'input'].get('component').encode('utf-8')

        if config[u'input'].has_key("host"):
            host = config[u'input'].get("host").encode('utf-8')
        else:
            host = socket.getfqdn()

        port = config[u'input'].get('port')
        https = config[u'input'].get('https')
        kafkaConfig = config[u'output'].get(u'kafka')
        brokerList = kafkaConfig.get('brokerList')
        topic = kafkaConfig.get('topic').encode('utf-8')

        beans = get_jmx_beans(host, port, https)
        #print brokerList
        kafka, producer = kafka_connect(brokerList)
        default_metric = {"site": site, "host": host, "timestamp": '', "component": component, "metric": '', "value": ''}
        fat_bean = dict()
        parse_hadoop_jmx(producer, topic, config, beans, default_metric, fat_bean)
        extend_jmx_metrics(producer, topic, default_metric, fat_bean)
    except Exception, e:
        print 'main except: ', e
    finally:
        if kafka != None and producer != None:
            kafka_close(kafka, producer)

        #elapsed = (time.clock() - start)
        #print("Time used:",elapsed)

if __name__ == "__main__":
    main()
