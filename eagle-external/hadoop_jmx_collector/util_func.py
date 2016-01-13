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
    kafka_dict["value"] = str(value)
    kafka_json = json.dumps(kafka_dict)

    if producer != None:
        producer.send_messages(topic, kafka_json)
    else:
        print(kafka_json)


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


