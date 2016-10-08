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
package org.apache.eagle.app;

import com.typesafe.config.Config;
import org.apache.eagle.app.environment.impl.StaticApplicationExecutor;
import org.apache.eagle.app.environment.impl.WebEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static Application without executable process.
 */
public class StaticApplication implements Application<WebEnvironment, StaticApplicationExecutor> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StaticApplication.class);
    private final String name;

    public StaticApplication(String name) {
        this.name = name;
    }

    @Override
    public StaticApplicationExecutor execute(Config config, WebEnvironment environment) {
        LOGGER.warn("Executing Static application");
        return new StaticApplicationExecutor(this);
    }

    @Override
    public Class<? extends WebEnvironment> getEnvironmentType() {
        return WebEnvironment.class;
    }

    @Override
    public boolean isExecutable() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("StaticApplication(%s)",this.name);
    }
}