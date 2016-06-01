/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.eagle.alert.engine;

import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.apache.eagle.alert.engine.coordinator.impl.ZKMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.runner.UnitTopologyRunner;

import backtype.storm.generated.StormTopology;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Since 5/3/16. Make sure unit topology can be started either from command line
 * or from remote A few parameters for starting unit topology 1. number of spout
 * tasks 2. number of router bolts 3. number of alert bolts 4. number of publish
 * bolts
 *
 * Connections 1. spout and router bolt 2. router bolt and alert bolt 3. alert
 * bolt and publish bolt
 */
public class UnitTopologyMain {

    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        String topologyId = config.getString("topology.name");
        ZKMetadataChangeNotifyService changeNotifyService = new ZKMetadataChangeNotifyService(zkConfig, topologyId);

        new UnitTopologyRunner(changeNotifyService).run(topologyId, config);
    }

    public static StormTopology createTopology(Config config) {
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        String topologyId = config.getString("topology.name");
        ZKMetadataChangeNotifyService changeNotifyService = new ZKMetadataChangeNotifyService(zkConfig, topologyId);

        return new UnitTopologyRunner(changeNotifyService).buildTopology(topologyId, config);
    }
}
