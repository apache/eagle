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
package org.apache.eagle.security.hive.config;

import org.apache.eagle.dataproc.impl.storm.zookeeper.ZKStateConfig;
import org.apache.eagle.jpm.util.JobIdPartitioner;

import java.io.Serializable;

public class RunningJobCrawlConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    public RunningJobEndpointConfig endPointConfig;
    public ControlConfig controlConfig;
    public ZKStateConfig zkStateConfig;

    public RunningJobCrawlConfig(RunningJobEndpointConfig endPointConfig, ControlConfig controlConfig, ZKStateConfig zkStateConfig) {
        this.endPointConfig = endPointConfig;
        this.controlConfig = controlConfig;
        this.zkStateConfig = zkStateConfig;
    }

    public static class RunningJobEndpointConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        public String[] RMBasePaths;
        public String HSBasePath;
    }

    public static class ControlConfig implements Serializable {
        private static final long serialVersionUID = 1L;
        public boolean jobConfigEnabled;
        public boolean jobInfoEnabled;
        public int zkCleanupTimeInday;
        public int completedJobOutofDateTimeInMin;
        public int sizeOfJobConfigQueue;
        public int sizeOfJobCompletedInfoQueue;
        public Class<? extends JobIdPartitioner> partitionerCls;
        public int numTotalPartitions = 1;
    }
}
