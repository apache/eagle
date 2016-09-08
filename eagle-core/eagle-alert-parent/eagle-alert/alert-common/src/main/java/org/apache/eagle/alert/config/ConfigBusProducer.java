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
package org.apache.eagle.alert.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;

public class ConfigBusProducer extends ConfigBusBase {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ConfigBusProducer.class);

    public ConfigBusProducer(ZKConfig config) {
        super(config);
    }

    public void send(String topic, ConfigValue config) {
        // check if topic exists, create this topic if not existing
        String zkPath = zkRoot + "/" + topic;
        try {
            if (curator.checkExists().forPath(zkPath) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(zkPath);
            }
            ObjectMapper mapper = new ObjectMapper();
            byte[] content = mapper.writeValueAsBytes(config);
            curator.setData().forPath(zkPath, content);
        } catch (Exception ex) {
            LOG.error("error creating zkPath " + zkPath, ex);
            throw new RuntimeException(ex);
        }
    }
}
