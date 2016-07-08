/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.eagle.alert.coordinator.trigger;

import java.util.concurrent.TimeUnit;

import org.apache.eagle.alert.config.ConfigBusProducer;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordinator.Coordinator;
import org.apache.eagle.alert.coordinator.IPolicyScheduler;
import org.apache.eagle.alert.coordinator.IScheduleContext;
import org.apache.eagle.alert.coordinator.PolicySchedulerFactory;
import org.apache.eagle.alert.coordinator.ScheduleOption;
import org.apache.eagle.alert.coordinator.TopologyMgmtService;
import org.apache.eagle.alert.coordinator.provider.ScheduleContextBuilder;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.typesafe.config.Config;

/**
 * @since Jun 27, 2016
 *
 */
public class CoordinatorTrigger implements Runnable {
    // TODO : support configurable in coordiantor
    public static final int INIT_PERIODICALLY_TRIGGER_DELAY = 6000;
    // 30 minutes a trigger by default
    public static final int INIT_PERIODICALLY_TRIGGER_INTERVAL = 1000 * 60 * 30;

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorTrigger.class);

    private Config config;
    private IMetadataServiceClient client;

    public CoordinatorTrigger(Config config, IMetadataServiceClient client) {
        this.config = config;
        this.client = client;
    }

    @Override
    public void run() {
        try {
            if (Coordinator.isPeriodicallyForceBuildEnable()) {
                LOG.info("CoordinatorTrigger started ... ");

                Stopwatch watch = Stopwatch.createStarted();

                // schedule
                IScheduleContext context = new ScheduleContextBuilder(client).buildContext();
                TopologyMgmtService mgmtService = new TopologyMgmtService();
                IPolicyScheduler scheduler = PolicySchedulerFactory.createScheduler();

                scheduler.init(context, mgmtService);

                ScheduleState state = scheduler.schedule(new ScheduleOption());

                ConfigBusProducer producer = new ConfigBusProducer(ZKConfigBuilder.getZKConfig(config));
                Coordinator.postSchedule(client, state, producer);

                watch.stop();
                LOG.info("CoordinatorTrigger ended, used time {} sm.", watch.elapsed(TimeUnit.MILLISECONDS));
            }  else {
                LOG.info("CoordinatorTrigger found isPeriodicallyForceBuildEnable = false, skipped build");
            }
        } catch (Exception e) {
            LOG.error("trigger schedule failed!", e);
        }
    }

}
