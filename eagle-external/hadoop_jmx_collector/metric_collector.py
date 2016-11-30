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

import os
import re
import time
import json
import urllib2
import sys
import socket
import types
import httplib
import logging
import threading

# load six
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '', 'lib/six'))
import six

# load kafka-python
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '', 'lib/kafka-python'))
from kafka import KafkaClient, SimpleProducer, SimpleConsumer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-6s %(message)s',
                    datefmt='%m-%d %H:%M')

class Helper:
    def __init__(self):
        pass

    @staticmethod
    def load_config(config_file="config.json"):
        """

        :param config_file:
        :return:
        """
        abs_file_path = config_file

        if not os.path.isfile(abs_file_path):
            script_dir = os.path.dirname(__file__)
            rel_path = "./" + config_file
            abs_file_path = os.path.join(script_dir, rel_path)
            if not os.path.isfile(abs_file_path):
                raise Exception(abs_file_path + " doesn't exist, please rename config-sample.json to config.json")

        logging.info("Using configuration file " + abs_file_path)
        f = open(abs_file_path, 'r')
        json_file = f.read()
        f.close()
        config = json.loads(json_file)
        return config

    @staticmethod
    def is_number(str):
        """

        :param str:
        :return:
        """
        try:
            if str == None or isinstance(str, (bool)):
                return False
            float(str)
            return True
        except:
            return False

    @staticmethod
    def http_get(host, port, https=False, path=None):
        """
        Read url by GET method

        :param path:
        :param url:
        :param https:
        :return:
        """
        url = ":".join([host, str(port)])
        result = None
        response = None
        try:
            if https:
                logging.info("Reading https://" + str(url) + path)
                c = httplib.HTTPSConnection(url, timeout=57)
                c.request("GET", path)
                response = c.getresponse()
            else:
                logging.info("Reading http://" + str(url) + path)
                response = urllib2.urlopen("http://" + str(url) + path, timeout=57)
            logging.debug("Got response")
            result = response.read()
        finally:
            if response is not None:
                response.close()
            return result


class JmxReader(object):
    def __init__(self, host, port, https=False):
        self.host = host
        self.port = port
        self.https = https
        self.jmx_json = None
        self.jmx_beans = None
        self.jmx_raw = None

    def open(self):
        """
        :return: JmxReader
        """
        self.read_raw()
        self.set_raw(self.jmx_raw)
        return self

    def read_raw(self):
        """
        transfer the json string into dict
        :param host:
        :param port:
        :param https:
        :return: text
        """
        self.jmx_raw = Helper.http_get(self.host, self.port, self.https, "/jmx?anonymous=true")
        if self.jmx_raw is None:
            raise Exception("Response from " + url + " is None")
        return self

    def set_raw(self, text):
        self.jmx_json = json.loads(text)
        self.jmx_beans = self.jmx_json[u'beans']
        self.jmx_raw = text
        return self

    def get_jmx_beans(self):
        return self.jmx_beans

    def get_jmx_bean_by_name(self, name):
        for bean in self.jmx_beans:
            if bean.has_key("name") and bean["name"] == name:
                return bean


class YarnWSReader:
    def __init__(self, host, port, https=False):
        self.host = host
        self.port = port
        self.https = https

    def read_cluster_info(self):
        cluster_info = Helper.http_get(self.host, self.port, self.https, "/ws/v1/cluster/info?anonymous=true")
        logging.debug(cluster_info)
        return json.loads(cluster_info)


class MetricSender(object):
    def __init__(self, config):
        pass

    def open(self):
        pass

    def send(self, msg):
        raise Exception("should be overrode")

    def close(self):
        pass


class KafkaMetricSender(MetricSender):
    def __init__(self, config):
        super(KafkaMetricSender, self).__init__(config)
        kafka_config = config["output"]["kafka"]
        # default topic
        self.default_topic = None
        if kafka_config.has_key("default_topic"):
            self.default_topic = kafka_config["default_topic"].encode('utf-8')
        self.component_topic_mapping = {}
        if kafka_config.has_key("component_topic_mapping"):
            self.component_topic_mapping = kafka_config["component_topic_mapping"]

        if not self.default_topic and not bool(self.component_topic_mapping):
            raise Exception("both kafka config 'topic' and 'component_topic_mapping' are empty")

        # producer
        self.broker_list = kafka_config["broker_list"]
        self.kafka_client = None
        self.kafka_producer = None

    def get_topic_id(self, msg):
        if msg.has_key("component"):
            component = msg["component"]
            if self.component_topic_mapping.has_key(component):
                return self.component_topic_mapping[component]
            else:
                return self.default_topic
        else:
            if not self.default_topic:
                raise Exception("no default topic found for unknown-component msg: " + str(msg))
            return self.default_topic

    def open(self):
        self.kafka_client = KafkaClient(self.broker_list, timeout=59)
        self.kafka_producer = SimpleProducer(self.kafka_client, batch_send=True, batch_send_every_n=500,
                                             batch_send_every_t=30)

    def send(self, msg):
        self.kafka_producer.send_messages(self.get_topic_id(msg), json.dumps(msg))

    def close(self):
        if self.kafka_producer is not None:
            self.kafka_producer.stop()
        if self.kafka_client is not None:
            self.kafka_client.close()

class MetricCollector(threading.Thread):
    def __init__(self,config = None):
        threading.Thread.__init__(self)
        self.config = None
        self.sender = None
        self.fqdn = socket.getfqdn()

    def init(self, config):
        self.config = config
        self.sender = KafkaMetricSender(self.config)
        self.sender.open()
        pass

    def start(self):
        super(MetricCollector, self).start()

    def collect(self, msg):
        if not msg.has_key("timestamp"):
            msg["timestamp"] = int(round(time.time() * 1000))
        if msg.has_key("value"):
            msg["value"] = float(str(msg["value"]))
        if not msg.has_key("host") or len(msg["host"]) == 0:
            msg["host"] = self.fqdn
        if not msg.has_key("site"):
            msg["site"] = self.config["env"]["site"]
        self.sender.send(msg)

    def close(self):
        self.sender.close()

    def run(self):
        raise Exception("`run` method should be overrode by sub-class before being called")

class Runner(object):
    @staticmethod
    def run(*collectors):
        """
        Execute concurrently

        :param threads:
        :return:
        """
        argv = sys.argv
        if len(argv) == 1:
            config = Helper.load_config()
        elif len(argv) == 2:
            config = Helper.load_config(argv[1])
        else:
            raise Exception("Usage: "+argv[0]+" CONFIG_FILE_PATH, but given too many arguments: " + str(argv))

        for collector in collectors:
            try:
                collector.init(config)
                collector.start()
            except Exception as e:
                logging.exception(e)

        for collector in collectors:
            try:
                collector.join()
            except Exception as e:
                logging.exception(e)
            finally:
                collector.close()

class JmxMetricCollector(MetricCollector):
    selected_domain = None
    listeners = []
    input_components = []

    def init(self, config):
        super(JmxMetricCollector, self).init(config)
        self.input_components = config["input"]
        for input in self.input_components:
            if not input.has_key("host"):
                input["host"] = self.fqdn
            if not input.has_key("component"):
                raise Exception("component not defined in " + str(input))
            if not input.has_key("port"):
                raise Exception("port not defined in " + str(input))
            if not input.has_key("https"):
                input["https"] = False

        self.selected_domain = [s.encode('utf-8') for s in config[u'filter'].get('monitoring.group.selected')]
        self.listeners = []

    def register(self, *listeners):
        """
        :param listeners: type of HadoopJmxListener
        :return:
        """
        for listener in listeners:
            listener.init(self)
            self.listeners.append(listener)

    def run(self):
        for input in self.input_components:
            try:
                beans = JmxReader(input["host"], input["port"], input["https"]).open().get_jmx_beans()
                self.on_beans(input["component"], beans)
            except Exception as e:
                logging.exception("Failed to read jmx for " + str(input))

    def filter_bean(self, bean, mbean_domain):
        return mbean_domain in self.selected_domain

    def on_beans(self, component, beans):
        for bean in beans:
            self.on_bean(component,bean)

    def on_bean(self, component, bean):
        # mbean is of the form "domain:key=value,...,foo=bar"
        mbean = bean[u'name']
        mbean_domain, mbean_attribute = mbean.rstrip().split(":", 1)
        mbean_domain = mbean_domain.lower()

        if not self.filter_bean(bean, mbean_domain):
            return

        context = bean.get("tag.Context", "")
        metric_prefix_name = self.__build_metric_prefix(mbean_attribute, context)

        # print kafka_dict
        for key, value in bean.iteritems():
            self.on_bean_kv(metric_prefix_name, component,key, value)

        for listener in self.listeners:
            listener.on_bean(component, bean.copy())

    def on_bean_kv(self, prefix,component, key, value):
        # Skip Tags
        if re.match(r'tag.*', key):
            return
        metric_name = (prefix + '.' + key).lower()
        self.on_metric({
            "component": component,
            "metric": metric_name,
            "value": value
        })

    def on_metric(self, metric):
        if Helper.is_number(metric["value"]):
            self.collect(metric)

        for listener in self.listeners:
            listener.on_metric(metric.copy())

    def __build_metric_prefix(self, mbean_attribute, context):
        mbean_list = list(prop.split("=", 1) for prop in mbean_attribute.split(","))
        if context == "":
            metric_prefix_name = '.'.join([i[1] for i in mbean_list])
        else:
            name_index = [i[0] for i in mbean_list].index('name')
            mbean_list[name_index][1] = context
            metric_prefix_name = '.'.join([i[1] for i in mbean_list])
        return ("hadoop." + metric_prefix_name).replace(" ", "").lower()


class JmxMetricListener:
    def init(self, collector):
        self.collector = collector

    def on_bean(self, component, bean):
        pass

    def on_metric(self, metric):
        pass