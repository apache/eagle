/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.engine;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.apache.eagle.alert.engine.coordinator.impl.ZKMetadataChangeNotifyService;
import org.apache.eagle.alert.engine.runner.UnitSparkTopologyRunner;

public class UnitSparkTopologyMain {
    public static void main(String[] args) throws InterruptedException {
        Config config = ConfigFactory.load();
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        String topologyId = config.getString("topology.name");
        ZKMetadataChangeNotifyService changeNotifyService = new ZKMetadataChangeNotifyService(zkConfig, topologyId);

        new UnitSparkTopologyRunner(changeNotifyService,config).run();
    }
}
