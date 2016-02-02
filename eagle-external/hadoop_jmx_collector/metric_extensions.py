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

from util_func import *
import json

# Metric Parsing Callback Entry
def single_metric_callback(producer, topic, kafka_dict, metric, value):
    if isNumber(value):
        numeric_metric_callack(producer, topic, kafka_dict, metric, value)
    else:
        nonnumeric_metric_callack(producer, topic, kafka_dict, metric, value)

def metrics_bean_callback(producer, topic, metric, bean):
    cal_mem_usage(producer, topic, bean, metric, "hadoop.namenode.jvm")
    journal_transaction_info(producer,topic,bean,metric,"hadoop.namenode.journaltransaction")

#################################################
# Metric Parsing Extensions
#################################################

def numeric_metric_callack(producer, topic, kafka_dict, metric, value):
    # Send out numeric value directly
    send_output_message(producer, topic, kafka_dict, metric, value)

def nonnumeric_metric_callack(producer, topic, kafka_dict, metric, value):
    nn_safe_mode_metric(producer, topic, kafka_dict, metric, value)

def nn_safe_mode_metric(producer, topic, kafka_dict, metric, value):
    if metric == "hadoop.namenode.fsnamesystemstate.fsstate":
        if value == "safeMode":
            value = 1
        else:
            value = 0

        send_output_message(producer, topic, kafka_dict, metric, value)

def cal_mem_usage(producer, topic, bean, metricMap, metric_prefix_name):
    kafka_dict = metricMap.copy()
    PercentVal = None
    PercentVal = round(float(bean['MemNonHeapUsedM']) / float(bean['MemNonHeapMaxM']) * 100.0, 2)
    send_output_message(producer, topic, kafka_dict, metric_prefix_name + ".MemNonHeapUsedUsage", PercentVal)

    PercentVal = round(float(bean['MemNonHeapCommittedM']) / float(bean['MemNonHeapMaxM']) * 100, 2)
    send_output_message(producer, topic, kafka_dict, metric_prefix_name + ".MemNonHeapCommittedUsage", PercentVal)

    PercentVal = round(float(bean['MemHeapUsedM']) / float(bean['MemHeapMaxM']) * 100, 2)
    send_output_message(producer, topic, kafka_dict, metric_prefix_name + ".MemHeapUsedUsage", PercentVal)

    PercentVal = round(float(bean['MemHeapCommittedM']) / float(bean['MemHeapMaxM']) * 100, 2)
    send_output_message(producer, topic, kafka_dict, metric_prefix_name + ".MemHeapCommittedUsage", PercentVal)

def journal_transaction_info(producer, topic, bean, metric, metric_prefix_name):
    new_metric = metric.copy()
    if bean.has_key("JournalTransactionInfo"):
        JournalTransactionInfo = json.loads(bean.get("JournalTransactionInfo"))

        LastAppliedOrWrittenTxId = int(JournalTransactionInfo.get("LastAppliedOrWrittenTxId"))
        MostRecentCheckpointTxId = int(JournalTransactionInfo.get("MostRecentCheckpointTxId"))

        send_output_message(producer, topic, new_metric, metric_prefix_name + ".LastAppliedOrWrittenTxId", LastAppliedOrWrittenTxId)
        send_output_message(producer, topic, new_metric, metric_prefix_name + ".MostRecentCheckpointTxId", MostRecentCheckpointTxId)
    else:
        raise Exception("JournalTransactionInfo not found")

