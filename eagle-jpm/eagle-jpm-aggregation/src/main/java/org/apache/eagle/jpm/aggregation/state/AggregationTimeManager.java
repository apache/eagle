/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.eagle.jpm.aggregation.state;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.eagle.jpm.aggregation.AggregationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregationTimeManager {
    private static final Logger LOG = LoggerFactory.getLogger(AggregationTimeManager.class);
    private String zkRoot;
    private CuratorFramework curator;

    public static final String ZNODE_LAST_UPDATE = "lastUpdateTime";

    private static AggregationTimeManager manager = new AggregationTimeManager();

    private AggregationTimeManager() {
    }

    public static AggregationTimeManager instance() {
        return manager;
    }

    private CuratorFramework newCurator(AggregationConfig.ZKStateConfig config) throws Exception {
        return CuratorFrameworkFactory.newClient(
            config.zkQuorum,
            config.zkSessionTimeoutMs,
            15000,
            new RetryNTimes(config.zkRetryTimes, config.zkRetryInterval)
        );
    }

    public void init(AggregationConfig.ZKStateConfig config) {
        this.zkRoot = config.zkRoot;
        try {
            curator = newCurator(config);
            curator.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        curator.close();
        curator = null;
    }

    public long readLastFinishTime() throws Exception {
        String path = zkRoot + "/" + ZNODE_LAST_UPDATE;
        try {
            if (curator.checkExists().forPath(path) != null) {
                return Long.parseLong(new String(curator.getData().forPath(path), "UTF-8"));
            } else {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, "0".getBytes("UTF-8"));
                return 0L;
            }
        } catch (Exception e) {
            LOG.error("fail reading lastUpdateTime {}", e);
            throw e;
        }
    }

    public void updateLastFinishTime(long lastUpdateTime) throws Exception {
        String path = zkRoot + "/" + ZNODE_LAST_UPDATE;
        try {
            if (curator.checkExists().forPath(path) == null) {
                curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path, (lastUpdateTime + "").getBytes("UTF-8"));
            } else {
                curator.setData().forPath(path, (lastUpdateTime + "").getBytes("UTF-8"));
            }
        } catch (Exception e) {
            LOG.error("fail update lastUpdateTime {}", e);
            throw e;
        }
    }
}
