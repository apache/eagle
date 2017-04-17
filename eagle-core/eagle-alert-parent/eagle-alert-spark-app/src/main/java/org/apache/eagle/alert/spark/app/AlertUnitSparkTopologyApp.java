/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eagle.alert.spark.app;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.runner.UnitSparkTopologyRunner;
import org.apache.eagle.alert.engine.runner.UnitSparkUnionTopologyRunner;
import org.apache.eagle.app.SparkApplication;
import org.apache.eagle.app.environment.impl.SparkEnvironment;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class AlertUnitSparkTopologyApp extends SparkApplication {
    private static final String TOPOLOGY_MULTIKAFKA = "topology.multikafka";

    @Override
    public JavaStreamingContext execute(Config config, SparkEnvironment environment) {
        boolean useMultiKafka = config.getBoolean(TOPOLOGY_MULTIKAFKA);
        if (useMultiKafka) {
            return new UnitSparkUnionTopologyRunner(config).buildTopology();
        }
        return new UnitSparkTopologyRunner(config).buildTopology();
    }
}
