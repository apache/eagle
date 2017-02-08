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

import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.apache.eagle.app.environment.AbstractEnvironment;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ScheduledEnvironment extends AbstractEnvironment {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledEnvironment.class);

    private final Scheduler scheduler;

    public ScheduledEnvironment(Config config) throws SchedulerException {
        super(config);
        this.scheduler = new StdSchedulerFactory().getScheduler();
        this.start();
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void start() throws SchedulerException {
        if (!this.scheduler.isStarted()) {
            LOGGER.info("Starting scheduler {}", this.scheduler.getSchedulerName());
            this.scheduler.start();
        } else {
            LOGGER.info("Scheduler {} already started", this.scheduler.getSchedulerName());
        }
    }

    public void stop() throws SchedulerException {
        if (this.scheduler.isShutdown()) {
            LOGGER.info("Shutting down scheduler {}", this.scheduler.getSchedulerName());
            this.scheduler.shutdown();
        } else {
            LOGGER.info("Scheduler {} already stopped", this.scheduler.getSchedulerName());
        }
    }
}