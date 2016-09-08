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
package org.apache.eagle.alert.tools;

import org.apache.eagle.alert.config.ZKConfig;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumerOffsetFetcher {
    public CuratorFramework curator;
    public String zkRoot;
    public ObjectMapper mapper;
    public String zkPathToPartition;

    public KafkaConsumerOffsetFetcher(ZKConfig config, String... parameters) {
        try {
            this.curator = CuratorFrameworkFactory.newClient(config.zkQuorum, config.zkSessionTimeoutMs, 15000,
                new RetryNTimes(config.zkRetryTimes, config.zkRetryInterval));
            curator.start();
            this.zkRoot = config.zkRoot;
            mapper = new ObjectMapper();
            Module module = new SimpleModule("offset").registerSubtypes(new NamedType(KafkaConsumerOffset.class));
            mapper.registerModule(module);
            zkPathToPartition = String.format(config.zkRoot, parameters);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, Long> fetch() throws Exception {
        Map<String, Long> map = new HashMap<String, Long>();
        if (curator.checkExists().forPath(zkPathToPartition) != null) {
            List<String> partitions = curator.getChildren().forPath(zkPathToPartition);
            for (String partition : partitions) {
                String partitionPath = zkPathToPartition + "/" + partition;
                String data = new String(curator.getData().forPath(partitionPath));
                KafkaConsumerOffset offset = mapper.readValue(data, KafkaConsumerOffset.class);
                map.put(partition, offset.offset);
            }
        }
        return map;
    }
}
