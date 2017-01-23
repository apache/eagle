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

package org.apache.eagle.alert.engine.spark.function;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.runner.UnitSparkUnionTopologyRunner;
import org.apache.eagle.alert.engine.spark.model.KafkaClusterInfo;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.Iterator;
import java.util.List;

public class RefreshClusterAndTopicFunction implements Function<scala.collection.immutable.List<KafkaClusterInfo>, scala.collection.immutable.List<KafkaClusterInfo>> {

    private static final Logger LOG = LoggerFactory.getLogger(RefreshClusterAndTopicFunction.class);
    private Config config;

    public RefreshClusterAndTopicFunction(Config config) {
        this.config = config;
    }

    @Override
    public scala.collection.immutable.List<KafkaClusterInfo> call(scala.collection.immutable.List<KafkaClusterInfo> cachedClusterInfo) throws Exception {
        Iterator<KafkaClusterInfo> javaList = UnitSparkUnionTopologyRunner.getKafkaCLusterInfoByCache(config, cachedClusterInfo).iterator();
        return JavaConversions.asScalaIterator(javaList).toList();
    }
}
