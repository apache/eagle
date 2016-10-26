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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.hadoop.queue;

import com.typesafe.config.Config;

import java.io.Serializable;

public class HadoopQueueRunningAppConfig implements Serializable {
    public static final HadoopQueueRunningAppConfig instance = new HadoopQueueRunningAppConfig();

    public Topology topology;
    public DataSourceConfig dataSourceConfig;
    public EagleProps eagleProps;

    private Config config = null;

    private HadoopQueueRunningAppConfig() {
        this.topology = new Topology();
        this.dataSourceConfig = new DataSourceConfig();
        this.eagleProps = new EagleProps();
        this.config = null;
    }

    public static class Topology implements Serializable {
        public int numOfParserTasks;
    }

    public static class DataSourceConfig implements Serializable {
        public String rMEndPoints;
        public String fetchIntervalSec;
    }

    public static class EagleProps implements Serializable {
        public String site;
        public EagleService eagleService;

        public EagleProps() {
            eagleService = new EagleService();
        }

        public static class EagleService implements Serializable {
            public String host;
            public int port;
            public String username;
            public String password;
        }
    }

    public static HadoopQueueRunningAppConfig getInstance(Config config) {
        if (config != null && instance.config == null) {
            synchronized (instance) {
                if (instance.config == null) {
                    instance.init(config);
                }
                return instance;
            }
        }
        return instance;
    }

    public Config getConfig() {
        return config;
    }

    private void init(Config config) {
        this.config = config;

        this.topology.numOfParserTasks = config.getInt("topology.numOfParserTasks");

        this.dataSourceConfig.rMEndPoints = config.getString("dataSourceConfig.rMEndPoints");
        this.dataSourceConfig.fetchIntervalSec = config.getString("dataSourceConfig.fetchIntervalSec");

        this.eagleProps.site = config.getString("siteId");
        this.eagleProps.eagleService.host = config.getString("service.host");
        this.eagleProps.eagleService.port = config.getInt("service.port");
        this.eagleProps.eagleService.username = config.getString("service.username");
        this.eagleProps.eagleService.password = config.getString("service.password");
    }
}
