/*
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
package org.apache.eagle.app.environment.impl;

import org.apache.eagle.app.environment.AbstractEnvironment;
import org.apache.eagle.app.environment.builder.ApplicationBuilder;
import org.apache.eagle.app.environment.builder.MetricDefinition;
import org.apache.eagle.app.environment.builder.TransformFunction;
import org.apache.eagle.app.environment.builder.TransformFunctionBolt;
import org.apache.eagle.app.messaging.*;
import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.StreamSourceConfig;

/**
 * Storm Execution Environment Context.
 */
public class StormEnvironment extends AbstractEnvironment {
    public StormEnvironment(Config envConfig) {
        super(envConfig);
    }

    // ----------------------------------
    // Classic Storm Topology Builder API
    // ----------------------------------
    public StormStreamSink getStreamSink(String streamId, Config config) {
        return ((StormStreamSink) stream().getSink(streamId,config));
    }

    public StormStreamSource getStreamSource(String streamId, Config config) {
        return (StormStreamSource) stream().getSource(streamId,config);
    }

    public MetricStreamPersist getMetricPersist(MetricDefinition metricDefinition, Config config) {
        return new MetricStreamPersist(metricDefinition, config);
    }

    public MetricSchemaGenerator getMetricSchemaGenerator(MetricDefinition metricDefinition, Config config) {
        return new MetricSchemaGenerator(metricDefinition, config);
    }

    public TransformFunctionBolt getTransformer(TransformFunction function) {
        return new TransformFunctionBolt(function);
    }

    // ----------------------------------
    // Fluent Storm App Builder API
    // ----------------------------------

    public ApplicationBuilder newApp(Config appConfig) {
        return new ApplicationBuilder(appConfig, this);
    }
}