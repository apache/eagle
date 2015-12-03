/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.metric.kafka;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.eagle.dataproc.impl.storm.zookeeper.ZKStateConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumerOffsetFetcher {

    public CuratorFramework curator;
    public String zkRoot;
    public ObjectMapper mapper;
    public String topic;
    public String group;

    public KafkaConsumerOffsetFetcher(ZKStateConfig config, String topic, String group) {
        try {
            this.curator = CuratorFrameworkFactory.newClient(config.zkQuorum, config.zkSessionTimeoutMs, 15000,
                    new RetryNTimes(config.zkRetryTimes, config.zkRetryInterval));
            curator.start();
            this.zkRoot = config.zkRoot;
            mapper = new ObjectMapper();
            Module module = new SimpleModule("offset").registerSubtypes(new NamedType(KafkaConsumerOffset.class));
            mapper.registerModule(module);
            this.topic = topic;
            this.group = group;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Long> fetch() throws Exception {
        Map<String, Long> map = new HashMap<String, Long>();
        String path = zkRoot + "/" + topic + "/" + group;
        if (curator.checkExists().forPath(path) != null) {
            List<String> partitions = curator.getChildren().forPath(path);
            for (String partition : partitions) {
                String partitionPath = path + "/" + partition;
                String data = new String(curator.getData().forPath(partitionPath));
                KafkaConsumerOffset offset = mapper.readValue(data, KafkaConsumerOffset.class);
                map.put(partition, offset.offset);
            }
        }
        return map;
    }
}
