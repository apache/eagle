/*
 *
 *    Licensed to the Apache Software Foundation (ASF) under one or more
 *    contributor license agreements.  See the NOTICE file distributed with
 *    this work for additional information regarding copyright ownership.
 *    The ASF licenses this file to You under the Apache License, Version 2.0
 *    (the "License"); you may not use this file except in compliance with
 *    the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.apache.eagle.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;

public class PartitionStrategyImpl implements PartitionStrategy {

    public DataDistributionDao dao;
    public PartitionAlgorithm algorithm;
    public Map<String, Integer> routingTable;
    public long lastRefreshTime;
    public long refreshInterval;
    public static long DEFAULT_REFRESH_INTERVAL = 60 * 60 * 1000;
    private final Logger LOG = LoggerFactory.getLogger(PartitionStrategyImpl.class);

    public PartitionStrategyImpl(DataDistributionDao dao, PartitionAlgorithm algorithm, long refreshInterval) {
        this.dao = dao;
        this.algorithm = algorithm;
        this.refreshInterval = refreshInterval;
    }

    public PartitionStrategyImpl(DataDistributionDao dao, PartitionAlgorithm algorithm) {
        this(dao, algorithm, DEFAULT_REFRESH_INTERVAL);
    }

    public boolean needRefresh() {
        if (System.currentTimeMillis() > lastRefreshTime + refreshInterval) {
            lastRefreshTime = System.currentTimeMillis();
            return true;
        }
        return false;
    }

    public Map<String, Integer> generateRoutingTable(int buckNum) {
        try {
            List<Weight> weights = dao.fetchDataDistribution();
            routingTable = algorithm.partition(weights, buckNum);
            return routingTable;
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public int balance(String key, int buckNum) {
        if (needRefresh()) {
            LOG.info("Going to refresh routing table");
            routingTable = generateRoutingTable(buckNum);
            LOG.info("Finish refresh routing table");
        }
        if (routingTable.containsKey(key)) {
            return routingTable.get(key);
        }
        else {
            return Math.abs(key.hashCode()) % buckNum;
        }
    }
}
