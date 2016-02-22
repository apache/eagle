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


# !/usr/bin/python

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

# load six
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '', 'lib/six'))
import six

# load kafka-python
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '', 'lib/kafka-python'))
from kafka import KafkaClient, SimpleProducer, SimpleConsumer

logging.basicConfig(level=logging.DEBUG,
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
        # read the self-defined filters
        script_dir = os.path.dirname(__file__)
        rel_path = "./" + config_file
        abs_file_path = os.path.join(script_dir, rel_path)
        if not os.path.isfile(abs_file_path):
            logging.error(abs_file_path + " doesn't exist, please rename config-sample.json to config.json")
            exit(1)
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
    def http_get(url, https=False):
        """
        Read url by GET method

        :param url:
        :param https:
        :return:
        """
        result = None
        response = None
        try:
            if https:
                logging.info("Reading https://" + str(url) + "/jmx?anonymous=true")
                c = httplib.HTTPSConnection(url, timeout=57)
                c.request("GET", "/jmx?anonymous=true")
                response = c.getresponse()
            else:
                logging.info("Reading http://" + str(url) + "/jmx?anonymous=true")
                response = urllib2.urlopen("http://" + str(url) + '/jmx?anonymous=true', timeout=57)
            logging.debug("Got response")
            # everything is fine
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
        self.read()

    def read(self):

        """
        transfer the json string into dict
        :param host:
        :param port:
        :param https:
        :return:
        """
        url = ":".join([self.host,str(self.port)])
        result = Helper.http_get(url, self.https)
        if result is None:
            raise Exception("Response from "+url+" is None")
        self.jmx_json = json.loads(result)
        self.jmx_beans = self.jmx_json[u'beans']

    def get_jmx_beans(self):
        return self.jmx_beans

    def get_jmx_bean_by_name(self, name):
        for bean in self.jmx_beans:
            if bean["name"] == name:
                return bean

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
        self.topic = kafka_config["topic"].encode('utf-8')
        # producer
        self.broker_list = kafka_config["brokerList"]
        self.kafka_client = None
        self.kafka_producer = None

    def open(self):
        self.kafka_client = KafkaClient(self.broker_list, timeout=59)
        self.kafka_producer = SimpleProducer(self.kafka_client, batch_send=True, batch_send_every_n=500,
                                             batch_send_every_t=30)

    def send(self, msg):
        self.kafka_producer.send_messages(self.topic, json.dumps(msg))

    def close(self):
        if self.kafka_producer is not None:
            self.kafka_producer.stop()
        if self.kafka_client is not None:
            self.kafka_client.close()


class MetricCollector(object):
    def __init__(self):
        self.config = Helper.load_config()
        self.sender = KafkaMetricSender(self.config)

    def start(self):
        try:
            self.sender.open()
            self.run()
        finally:
            self.sender.close()

    def collect(self, msg):
        if not msg.has_key("timestamp"):
            msg["timestamp"] = int(round(time.time() * 1000))
        if msg.has_key("value"):
            msg["value"] = float(str(msg["value"]))

        self.sender.send(msg)

    def run(self):
        raise Exception("`run` method should be overrode by sub-class before being called")