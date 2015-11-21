/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle.metric.kafka;

import org.apache.eagle.dataproc.impl.storm.zookeeper.ZKStateConfig;

import java.io.Serializable;

public class KafkaOffsetCheckerConfig implements Serializable {
    public static class KafkaConfig implements Serializable{
        public String kafkaEndPoints;
        public String topic;
        public String site;
        public String group;
    }

    public static class ServiceConfig implements Serializable{
        public String serviceHost;
        public Integer servicePort;
        public String username;
        public String password;
    }

    public ZKStateConfig zkConfig;
    public KafkaConfig kafkaConfig;
    public ServiceConfig serviceConfig;

    public KafkaOffsetCheckerConfig (ServiceConfig serviceConfig, ZKStateConfig zkConfig, KafkaConfig kafkaConfig) {
        this.serviceConfig = serviceConfig;
        this.zkConfig = zkConfig;
        this.kafkaConfig = kafkaConfig;
    }
}
