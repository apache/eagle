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
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.slf4j.Logger;

/**
 * 1. When consumer is started, it always get notified of config
 * 2. When config is changed, consumer always get notified of config change.
 * Reliability issue:
 * TODO How to ensure config change message is always delivered to consumer
 */
public class ConfigBusConsumer extends ConfigBusBase {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ConfigBusConsumer.class);

    private NodeCache cache;
    private String zkPath;

    public ConfigBusConsumer(ZKConfig config, String topic, ConfigChangeCallback callback) {
        super(config);
        this.zkPath = zkRoot + "/" + topic;
        LOG.info("monitor change for zkPath " + zkPath);
        cache = new NodeCache(curator, zkPath);
        cache.getListenable().addListener(() -> {
                // get node value and notify callback
                ConfigValue v = getConfigValue();
                callback.onNewConfig(v);
            }
        );
        try {
            cache.start();
        } catch (Exception ex) {
            LOG.error("error start NodeCache listener", ex);
            throw new RuntimeException(ex);
        }
    }

    public ConfigValue getConfigValue() throws Exception {
        byte[] value = curator.getData().forPath(zkPath);
        ObjectMapper mapper = new ObjectMapper();
        ConfigValue v = mapper.readValue(value, ConfigValue.class);
        return v;
    }
}
