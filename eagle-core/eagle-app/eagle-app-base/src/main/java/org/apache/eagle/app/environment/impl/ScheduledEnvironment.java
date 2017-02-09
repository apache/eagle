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
package org.apache.eagle.app.environment.impl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import io.dropwizard.lifecycle.Managed;
import org.apache.eagle.app.environment.AbstractEnvironment;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

@Singleton
public class ScheduledEnvironment extends AbstractEnvironment {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledEnvironment.class);

    private final Scheduler scheduler;

    @Inject
    public ScheduledEnvironment(Config config) throws SchedulerException {
        super(config);
        this.scheduler = new StdSchedulerFactory().getScheduler();
    }

    public Scheduler scheduler() {
        return scheduler;
    }
}