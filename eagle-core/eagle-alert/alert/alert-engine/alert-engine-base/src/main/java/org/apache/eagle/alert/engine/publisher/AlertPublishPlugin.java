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
package org.apache.eagle.alert.engine.publisher;

import org.apache.eagle.alert.engine.coordinator.Publishment;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.eagle.alert.engine.publisher.impl.PublishStatus;

import com.typesafe.config.Config;

import java.io.Closeable;
import java.util.Map;

/**
 * Created on 2/10/16.
 * Notification Plug-in interface which provide abstraction layer to notify to different system
 */
public interface AlertPublishPlugin extends Closeable {
    /**
     * for initialization
     * @throws Exception
     */
    void init(Config config, Publishment publishment) throws Exception;

    void update(String dedupIntervalMin, Map<String, String> pluginProperties);

    void close();

    void onAlert(AlertStreamEvent event) throws Exception;

    AlertStreamEvent dedup(AlertStreamEvent event);

    PublishStatus getStatus();

}
