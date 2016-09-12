/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.eagle.alert.engine.publisher.impl;

import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.publisher.AlertPublishPlugin;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @since on 5/11/16.
 */
public class AlertPublishPluginsFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AlertPublishPluginsFactory.class);

    @SuppressWarnings("rawtypes")
    public static AlertPublishPlugin createNotificationPlugin(Publishment publishment, Config config, Map conf) {
        AlertPublishPlugin plugin = null;
        String publisherType = publishment.getType();
        try {
            plugin = (AlertPublishPlugin) Class.forName(publisherType).newInstance();
            plugin.init(config, publishment, conf);
        } catch (Exception ex) {
            LOG.error("Error in loading AlertPublisherPlugin class: ", ex);
            //throw new IllegalStateException(ex);
        }
        return plugin;
    }

}
