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

import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeProvider;
import com.typesafe.config.Config;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkExecutionRuntime implements ExecutionRuntime<SparkEnvironment, JavaStreamingContext> {


    private static final Logger LOG = LoggerFactory.getLogger(SparkExecutionRuntime.class);
    private SparkEnvironment environment;
    private JavaStreamingContext jssc;

    @Override
    public void prepare(SparkEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public SparkEnvironment environment() {
        return this.environment;
    }

    @Override
    public void start(Application<SparkEnvironment, JavaStreamingContext> executor, Config config) {
        jssc = executor.execute(config, environment);
        LOG.info("Starting Spark Streaming");
        jssc.start();
        LOG.info("Spark Streaming is running");
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            LOG.error("SparkExecutionRuntime  jssc.awaitTermination throw exception", e);
        }
    }

    @Override
    public void stop(Application<SparkEnvironment, JavaStreamingContext> executor, Config config) {
        jssc.stop();
    }

    @Override
    public ApplicationEntity.Status status(Application<SparkEnvironment, JavaStreamingContext> executor, Config config) {
        ApplicationEntity.Status status = null;
        if (status == null) {
            LOG.error("Unknown storm topology  status res is null");
            status = ApplicationEntity.Status.INITIALIZED;
            return status;
        }
        StreamingContextState state = jssc.getState();

        if (state == StreamingContextState.ACTIVE) {
            status = ApplicationEntity.Status.RUNNING;
        } else if (state == StreamingContextState.STOPPED) {
            return ApplicationEntity.Status.STOPPED;
        } else if (state == StreamingContextState.INITIALIZED) {
            return ApplicationEntity.Status.INITIALIZED;
        } else {
            LOG.error("Unknown storm topology  status");
            status = ApplicationEntity.Status.UNKNOWN;
        }
        return status;
    }

    public static class Provider implements ExecutionRuntimeProvider<SparkEnvironment, JavaStreamingContext> {
        @Override
        public SparkExecutionRuntime get() {
            return new SparkExecutionRuntime();
        }
    }
}