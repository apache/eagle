/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.app.environment.builder;


import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.eagle.app.environment.impl.StormEnvironment;
import org.apache.eagle.app.messaging.MetricSchemaGenerator;
import org.apache.eagle.app.messaging.MetricStreamPersist;
import org.apache.eagle.app.messaging.StormStreamSource;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Storm Application Builder DSL.
 */
public class ApplicationBuilder {
    private final StormEnvironment environment;
    private final Config appConfig;
    private final TopologyBuilder topologyBuilder;
    private final AtomicInteger identifier;

    public ApplicationBuilder(Config appConfig, StormEnvironment environment) {
        this.appConfig = appConfig;
        this.environment = environment;
        this.identifier = new AtomicInteger(0);
        this.topologyBuilder = new TopologyBuilder();
    }

    public class BuilderContext {
        public StormTopology toTopology() {
            return topologyBuilder.createTopology();
        }

        public SourcedStream fromStream(String streamId) {
            return ApplicationBuilder.this.fromStream(streamId);
        }
    }

    public abstract class InitializedStream extends BuilderContext {
        private String id;

        InitializedStream(String id) {
            Preconditions.checkNotNull(id);
            this.id = id;
        }

        String getId() {
            return this.id;
        }

        /**
         * Persist source data stream as metric.
         */
        public BuilderContext saveAsMetric(MetricDescriptor metricDescriptor) {
            String metricDataID = generateId("MetricDataSink");
            String metricSchemaID = generateId("MetricSchemaGenerator");
            topologyBuilder.setBolt(metricDataID, new MetricStreamPersist(metricDescriptor, appConfig)).shuffleGrouping(getId());
            topologyBuilder.setBolt(metricSchemaID, new MetricSchemaGenerator(metricDescriptor,appConfig)).fieldsGrouping(metricDataID,new Fields(MetricStreamPersist.METRIC_NAME_FIELD));
            return this;
        }

        public TransformedStream transformBy(TransformFunction function) {
            String componentId = generateId(function.getName());
            topologyBuilder.setBolt(componentId, new TransformFunctionBolt(function)).shuffleGrouping(getId());
            return new TransformedStream(componentId);
        }
    }

    public class SourcedStream extends InitializedStream {
        private final Config appConfig;
        private final StormStreamSource streamSource;

        private SourcedStream(SourcedStream withSourcedStream) {
            this(withSourcedStream.getId(), withSourcedStream.appConfig, withSourcedStream.streamSource);
        }

        private SourcedStream(String componentId, Config appConfig, StormStreamSource streamSource) {
            super(componentId);
            this.appConfig = appConfig;
            this.streamSource = streamSource;
            topologyBuilder.setSpout(componentId, streamSource);
        }
    }

    public class TransformedStream extends InitializedStream {
        public TransformedStream(String id) {
            super(id);
        }
    }

    public TopologyBuilder getTopologyBuilder() {
        return this.topologyBuilder;
    }

    public StormTopology createTopology() {
        return topologyBuilder.createTopology();
    }


    public SourcedStream fromStream(String streamId) {
        return new SourcedStream(generateId("SourcedStream-" + streamId), this.appConfig, environment.getStreamSource(streamId, this.appConfig));
    }

    public SourcedStream fromStream(SourcedStream sourcedStream) {
        return new SourcedStream(sourcedStream);
    }

    private String generateId(String prefix) {
        return String.format("%s_%s", prefix, this.identifier.getAndIncrement());
    }
}