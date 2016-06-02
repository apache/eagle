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

import java.io.Closeable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;


/**
 * Abstraction of asynchronized configuration management
 * This is used for config change notification between processes, without this one process has to pull changes triggered by another process
 *
 * Config bus is similar to message bus, config change producer can publish config change(message) to config bus,
 *  while config change consumer can subscribe config change and do business logic in callback
 * 1. use zookeeper as media to notify config consumer of config changes
 * 2. each type of config is represented by topic
 * 3. each config change can contain actual value or contain reference Id which consumer uses to retrieve actual value. This mechanism will reduce zookeeper overhed
 *
 */
public class ConfigBusBase implements Closeable{
    protected String zkRoot;
    protected CuratorFramework curator;

    public ConfigBusBase(ZKConfig config) {
        this.zkRoot = config.zkRoot;
        curator = CuratorFrameworkFactory.newClient(
                config.zkQuorum,
                config.zkSessionTimeoutMs,
                config.connectionTimeoutMs,
                new RetryNTimes(config.zkRetryTimes, config.zkRetryInterval)
        );
        curator.start();
    }

    @Override
    public void close(){
        curator.close();
    }
}
