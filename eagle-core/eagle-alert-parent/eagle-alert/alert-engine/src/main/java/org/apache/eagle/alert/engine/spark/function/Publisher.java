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
import org.apache.eagle.alert.coordination.model.PublishSpec;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.alert.engine.model.AlertStreamEvent;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Map;


public class Publisher implements VoidFunction<JavaPairRDD<String, AlertStreamEvent>> {

    private PublishSpec publishSpec;
    private Map<String, StreamDefinition> sds;
    private String alertPublishBoltName;
    private Config config;

    public Publisher(PublishSpec publishSpec, Map<String, StreamDefinition> sds, String alertPublishBoltName) {
        this.publishSpec = publishSpec;
        this.sds = sds;
        this.alertPublishBoltName = alertPublishBoltName;
    }

    public Publisher(Config config, String alertPublishBoltName) {
        this.config = config;
        this.alertPublishBoltName = alertPublishBoltName;
    }

    @Override
    public void call(JavaPairRDD<String, AlertStreamEvent> rdd) throws Exception {
        rdd.foreachPartition(new AlertPublisherBoltFunction(config, alertPublishBoltName));
    }
}
