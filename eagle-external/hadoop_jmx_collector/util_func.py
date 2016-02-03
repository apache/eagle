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


#!/usr/bin/python

import os
import re
import time
import json

from kafka import KafkaClient, SimpleProducer, SimpleConsumer

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


def send_output_message(producer, topic, kafka_dict, metric, value):
    kafka_dict["timestamp"] = int(round(time.time() * 1000))
    kafka_dict["metric"] = metric.lower()
    kafka_dict["value"] = float(str(value))
    kafka_json = json.dumps(kafka_dict)

    if producer != None:
        producer.send_messages(topic, kafka_json)
    else:
        print(kafka_json)

def load_config(filename):
    # read the self-defined filters

    try:
        script_dir = os.path.dirname(__file__)
        rel_path = "./" + filename
        abs_file_path = os.path.join(script_dir, rel_path)
        if not os.path.isfile(abs_file_path):
            print abs_file_path+" doesn't exist, please rename config-sample.json to config.json"
            exit(1)
        f = open(abs_file_path, 'r')
        json_file = f.read()
        f.close()
        config = json.loads(json_file)
    except ValueError:
        print "configuration file load error"
    return config

def isNumber(str):
    try:
        if str == None or isinstance(str, (bool)):
            return False
        float(str)
        return True
    except:
        return False


