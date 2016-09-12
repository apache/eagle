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
package org.apache.eagle.app.environment;

import org.apache.eagle.app.sink.KafkaStreamSink;
import org.apache.eagle.app.sink.StreamSinkProvider;
import com.typesafe.config.Config;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractEnvironment implements Environment {
    private final Config config;
    private final StreamSinkProvider sinkProvider;
    private static final String APPLICATIONS_SINK_TYPE_PROPS_KEY = "application.sink.provider";
    private static final String DEFAULT_APPLICATIONS_SINK_TYPE = KafkaStreamSink.Provider.class.getName();
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEnvironment.class);

    public AbstractEnvironment(Config config) {
        this.config = config;
        this.sinkProvider = loadStreamSinkProvider();
    }

    private StreamSinkProvider loadStreamSinkProvider() {
        String sinkProviderClassName = config.hasPath(APPLICATIONS_SINK_TYPE_PROPS_KEY)
            ? config.getString(APPLICATIONS_SINK_TYPE_PROPS_KEY) : DEFAULT_APPLICATIONS_SINK_TYPE;
        try {
            Class<?> sinkProviderClass = Class.forName(sinkProviderClassName);
            if (!StreamSinkProvider.class.isAssignableFrom(sinkProviderClass)) {
                throw new IllegalStateException(sinkProviderClassName + "is not assignable from " + StreamSinkProvider.class.getCanonicalName());
            }
            StreamSinkProvider instance = (StreamSinkProvider) sinkProviderClass.newInstance();
            LOGGER.info("Loaded {}", instance);
            return instance;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOGGER.error(e.getMessage(), e);
            throw new IllegalStateException(e.getMessage(), e.getCause());
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
            .append(this.getClass())
            .append(this.config()).build();
    }

    @Override
    public StreamSinkProvider streamSink() {
        return sinkProvider;
    }

    @Override
    public Config config() {
        return config;
    }
}
