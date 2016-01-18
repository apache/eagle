#!/usr/bin/env python

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

# Eagle - Git Ignore Configuration
#
# See: https://github.com/github/gitignore


import threading, logging, time

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

class Producer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        producer = SimpleProducer(client)

        while True:
            producer.send_messages('my-topic', "test")
            producer.send_messages('my-topic', "\xc2Hola, mundo!")

            time.sleep(1)


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        client = KafkaClient("localhost:9092")
        consumer = SimpleConsumer(client, "test-group", "my-topic")

        for message in consumer:
            print(message)

def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
    main()
