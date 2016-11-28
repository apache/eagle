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

import org.apache.eagle.app.messaging.*;
import com.typesafe.config.Config;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractEnvironment implements Environment {

    private final Config config;
    private final StreamMessaging streamMessaging;
    private static final String APPLICATIONS_MESSAGING_TYPE_PROPS_KEY = "application.messaging.type";
    private static final String DEFAULT_APPLICATIONS_MESSAGING_TYPE = KafkaStreamMessaging.class.getName();
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEnvironment.class);

    public AbstractEnvironment(Config config) {
        this.config = config;
        this.streamMessaging = loadStreamMessaging();
    }

    private StreamMessaging loadStreamMessaging() {
        String sinkProviderClassName = config.hasPath(APPLICATIONS_MESSAGING_TYPE_PROPS_KEY)
            ? config.getString(APPLICATIONS_MESSAGING_TYPE_PROPS_KEY) : DEFAULT_APPLICATIONS_MESSAGING_TYPE;
        try {
            Class<?> sinkProviderClass = Class.forName(sinkProviderClassName);
            if (!StreamMessaging.class.isAssignableFrom(sinkProviderClass)) {
                throw new IllegalStateException(sinkProviderClassName + "is not assignable from " + StreamMessaging.class.getCanonicalName());
            }
            StreamMessaging instance = (StreamMessaging) sinkProviderClass.newInstance();
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

    public StreamMessaging stream() {
        return streamMessaging;
    }


    @Override
    public Config config() {
        return config;
    }
}