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
package org.apache.eagle.alert.coordinator;

import com.google.common.base.Stopwatch;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.eagle.alert.config.ConfigBusProducer;
import org.apache.eagle.alert.config.ConfigValue;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordinator.impl.MetadataValdiator;
import org.apache.eagle.alert.coordinator.provider.ScheduleContextBuilder;
import org.apache.eagle.alert.coordinator.trigger.CoordinatorTrigger;
import org.apache.eagle.alert.coordinator.trigger.DynamicPolicyLoader;
import org.apache.eagle.alert.coordinator.trigger.PolicyChangeListener;
import org.apache.eagle.alert.coordinator.trigger.ScheduleStateCleaner;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @since Mar 24, 2016.
 */
public class Coordinator {

    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);

    private static final String COORDINATOR = "coordinator";
    
    /**
     * /alert/{topologyName}/spout
     * /router
     * /alert
     * /publisher
     * .
     */
    private static final String ZK_ALERT_CONFIG_SPOUT = "{0}/spout";
    private static final String ZK_ALERT_CONFIG_ROUTER = "{0}/router";
    private static final String ZK_ALERT_CONFIG_ALERT = "{0}/alert";
    private static final String ZK_ALERT_CONFIG_PUBLISHER = "{0}/publisher";


    private static final String METADATA_SERVICE_HOST = "metadataService.host";
    private static final String METADATA_SERVICE_PORT = "metadataService.port";
    private static final String METADATA_SERVICE_CONTEXT = "metadataService.context";
    private static final String DYNAMIC_POLICY_LOADER_INIT_MILLS = "metadataDynamicCheck.initDelayMillis";
    private static final String DYNAMIC_POLICY_LOADER_DELAY_MILLS = "metadataDynamicCheck.delayMillis";
    private static final String DYNAMIC_SCHEDULE_STATE_CLEAR_MIN = "metadataDynamicCheck.stateClearPeriodMin";
    private static final String DYNAMIC_SCHEDULE_STATE_RESERVE_CAPACITY = "metadataDynamicCheck.stateReservedCapacity";

    private static final int DEFAULT_STATE_RESERVE_CAPACITY = 1000;

    public static final String GREEDY_SCHEDULER_ZK_PATH = "/alert/greedy/leader";

    private volatile ScheduleState currentState = null;
    private ZKConfig zkConfig = null;
    private final IMetadataServiceClient client;
    private Config config;

    // FIXME : UGLY global state
    private static final AtomicBoolean forcePeriodicallyBuild = new AtomicBoolean(true);

    public Coordinator() {
        config = ConfigFactory.load().getConfig(COORDINATOR);
        zkConfig = ZKConfigBuilder.getZKConfig(config);
        client = new MetadataServiceClientImpl(config);
    }

    public Coordinator(Config config, ZKConfig zkConfig, IMetadataServiceClient client) {
        this.config = config;
        this.zkConfig = zkConfig;
        this.client = client;
    }

    public synchronized ScheduleState schedule(ScheduleOption option) throws TimeoutException {
        ExclusiveExecutor executor = new ExclusiveExecutor(zkConfig);
        AtomicReference<ScheduleState> reference = new AtomicReference<>();
        try {
            executor.execute(GREEDY_SCHEDULER_ZK_PATH, () -> {
                ScheduleState state = null;
                Stopwatch watch = Stopwatch.createStarted();
                IScheduleContext context = new ScheduleContextBuilder(config, client).buildContext();
                TopologyMgmtService mgmtService = new TopologyMgmtService();
                IPolicyScheduler scheduler = PolicySchedulerFactory.createScheduler();

                scheduler.init(context, mgmtService);
                state = scheduler.schedule(option);

                long scheduleTime = watch.elapsed(TimeUnit.MILLISECONDS);
                state.setScheduleTimeMillis((int) scheduleTime);// hardcode to integer
                watch.reset();
                watch.start();

                // persist & notify
                try (ConfigBusProducer producer = new ConfigBusProducer(ZKConfigBuilder.getZKConfig(config))) {
                    postSchedule(client, state, producer);
                }

                watch.stop();
                long postTime = watch.elapsed(TimeUnit.MILLISECONDS);
                LOG.info("Schedule result, schedule time {} ms, post schedule time {} ms !", scheduleTime, postTime);
                reference.set(state);
                currentState = state;
            });
        } catch (TimeoutException e1) {
            LOG.error("time out when schedule", e1);
            throw e1;
        } finally {
            try {
                executor.close();
            } catch (IOException e) {
                LOG.error("Exception when close exclusive executor, log and ignore!", e);
            }
        }
        return reference.get();
    }

    public static void postSchedule(IMetadataServiceClient client, ScheduleState state, ConfigBusProducer producer) {
        // persist state
        client.addScheduleState(state);

        // notify
        ConfigValue value = new ConfigValue();
        value.setValue(state.getVersion());
        value.setValueVersionId(true);
        for (String topo : state.getSpoutSpecs().keySet()) {
            producer.send(MessageFormat.format(ZK_ALERT_CONFIG_SPOUT, topo), value);
        }
        for (String topo : state.getGroupSpecs().keySet()) {
            producer.send(MessageFormat.format(ZK_ALERT_CONFIG_ROUTER, topo), value);
        }
        for (String topo : state.getAlertSpecs().keySet()) {
            producer.send(MessageFormat.format(ZK_ALERT_CONFIG_ALERT, topo), value);
        }
        for (String topo : state.getPublishSpecs().keySet()) {
            producer.send(MessageFormat.format(ZK_ALERT_CONFIG_PUBLISHER, topo), value);
        }

    }

    public ScheduleState getState() {
        return currentState;
    }

    public ValidateState validate() {
        return new MetadataValdiator(client).validate();
    }

    /**
     * shutdown background threads and release various resources.
     */
    private static class CoordinatorShutdownHook implements Runnable {
        private static final Logger LOG = LoggerFactory.getLogger(CoordinatorShutdownHook.class);
        private ScheduledExecutorService executorSrv;

        public CoordinatorShutdownHook(ScheduledExecutorService executorSrv) {
            this.executorSrv = executorSrv;
        }

        @Override
        public void run() {
            LOG.info("start shutdown coordinator ...");
            LOG.info("Step 1 shutdown dynamic policy loader thread ");
            // we should catch every exception to make best effort for clean
            // shutdown
            try {
                executorSrv.shutdown();
                executorSrv.awaitTermination(2000, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                LOG.error("error shutdown dynamic policy loader", t);
            } finally {
                executorSrv.shutdownNow();
            }
        }
    }

    private static class PolicyChangeHandler implements PolicyChangeListener {
        private static final Logger LOG = LoggerFactory.getLogger(PolicyChangeHandler.class);
        private Config config;
        private IMetadataServiceClient client;

        public PolicyChangeHandler(Config config, IMetadataServiceClient client) {
            this.config = config;
            this.client = client;
        }

        @Override
        public void onPolicyChange(List<PolicyDefinition> allPolicies, Collection<String> addedPolicies,
                                   Collection<String> removedPolicies, Collection<String> modifiedPolicies) {
            LOG.info("policy changed ... ");
            LOG.info("allPolicies: " + allPolicies + ", addedPolicies: " + addedPolicies + ", removedPolicies: "
                    + removedPolicies + ", modifiedPolicies: " + modifiedPolicies);

            CoordinatorTrigger trigger = new CoordinatorTrigger(config, client);
            trigger.run();

        }
    }

    public static void startSchedule() {
        Config config = ConfigFactory.load().getConfig(COORDINATOR);
        String host = config.getString(METADATA_SERVICE_HOST);
        int port = config.getInt(METADATA_SERVICE_PORT);
        String context = config.getString(METADATA_SERVICE_CONTEXT);
        IMetadataServiceClient client = new MetadataServiceClientImpl(host, port, context);

        // schedule dynamic policy loader
        long initDelayMillis = config.getLong(DYNAMIC_POLICY_LOADER_INIT_MILLS);
        long delayMillis = config.getLong(DYNAMIC_POLICY_LOADER_DELAY_MILLS);
        ScheduledExecutorService scheduleSrv = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        DynamicPolicyLoader loader = new DynamicPolicyLoader(client);
        loader.addPolicyChangeListener(new PolicyChangeHandler(config, client));
        scheduleSrv.scheduleAtFixedRate(loader, initDelayMillis, delayMillis, TimeUnit.MILLISECONDS);

        if (config.hasPath(DYNAMIC_SCHEDULE_STATE_CLEAR_MIN) && config.hasPath(DYNAMIC_SCHEDULE_STATE_RESERVE_CAPACITY)) {
            int period = config.getInt(DYNAMIC_SCHEDULE_STATE_CLEAR_MIN);
            int capacity = config.getInt(DYNAMIC_SCHEDULE_STATE_RESERVE_CAPACITY);
            ScheduleStateCleaner cleaner = new ScheduleStateCleaner(client, capacity);
            scheduleSrv.scheduleAtFixedRate(cleaner, period, period, TimeUnit.MINUTES);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new CoordinatorShutdownHook(scheduleSrv)));
        LOG.info("Eagle Coordinator started ...");
    }

    public void enforcePeriodicallyBuild() {
        forcePeriodicallyBuild.set(true);
    }

    public void disablePeriodicallyBuild() {
        forcePeriodicallyBuild.set(false);
    }

    public static boolean isPeriodicallyForceBuildEnable() {
        return forcePeriodicallyBuild.get();
    }

}
