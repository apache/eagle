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

import java.text.MessageFormat;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.eagle.alert.config.ConfigBusProducer;
import org.apache.eagle.alert.config.ConfigValue;
import org.apache.eagle.alert.config.ZKConfig;
import org.apache.eagle.alert.config.ZKConfigBuilder;
import org.apache.eagle.alert.coordination.model.ScheduleState;
import org.apache.eagle.alert.coordinator.provider.ScheduleContextBuilder;
import org.apache.eagle.alert.coordinator.trigger.DynamicPolicyLoader;
import org.apache.eagle.alert.coordinator.trigger.PolicyChangeListener;
import org.apache.eagle.alert.engine.coordinator.PolicyDefinition;
import org.apache.eagle.alert.service.IMetadataServiceClient;
import org.apache.eagle.alert.service.MetadataServiceClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @since Mar 24, 2016 Coordinator is a standalone java application, which
 *        listens to policy changes and use schedule algorithm to distribute
 *        policies 1) reacting to shutdown events 2) start non-daemon thread to
 *        pull policies and figure out if polices are changed
 */
public class Coordinator {
    private static final String COORDINATOR = "coordinator";
    /**
     * {@link ZKMetadataChangeNotifyService}
     *  /alert/{topologyName}/spout
     *                  /router
     *                  /alert
     *                  /publisher
     */
    private static final String ZK_ALERT_CONFIG_SPOUT = "{0}/spout";
    private static final String ZK_ALERT_CONFIG_ROUTER = "{0}/router";
    private static final String ZK_ALERT_CONFIG_ALERT = "{0}/alert";
    private static final String ZK_ALERT_CONFIG_PUBLISHER = "{0}/publisher";

    private static final Logger LOG = LoggerFactory.getLogger(Coordinator.class);

    private final static String METADATA_SERVICE_HOST = "metadataService.host";
    private final static String METADATA_SERVICE_PORT = "metadataService.port";
    private final static String METADATA_SERVICE_CONTEXT = "metadataService.context";
    private final static String DYNAMIC_POLICY_LOADER_INIT_MILLS = "metadataDynamicCheck.initDelayMillis";
    private final static String DYNAMIC_POLICY_LOADER_DELAY_MILLS = "metadataDynamicCheck.delayMillis";

    private volatile ScheduleState currentState = null;
    private final ConfigBusProducer producer;
    private final IMetadataServiceClient client;
    private Config config;
    
    public Coordinator() {
        config = ConfigFactory.load().getConfig(COORDINATOR);
        ZKConfig zkConfig = ZKConfigBuilder.getZKConfig(config);
        producer = new ConfigBusProducer(zkConfig);
        client = new MetadataServiceClientImpl(config);
    }

    public Coordinator(Config config, ConfigBusProducer producer, IMetadataServiceClient client) {
        this.config = config;
        this.producer = producer;
        this.client = client;
    }

    public ScheduleState schedule(ScheduleOption option) {
        IScheduleContext context = new ScheduleContextBuilder(client).buildContext();
        TopologyMgmtService mgmtService = new TopologyMgmtService();
        IPolicyScheduler scheduler = PolicySchedulerFactory.createScheduler();

        scheduler.init(context, mgmtService);
        ScheduleState state = scheduler.schedule(option);

        // persist & notify
        postSchedule(client, state, producer);

        currentState = state;
        return state;
    }

    public static void postSchedule(IMetadataServiceClient client, ScheduleState state, ConfigBusProducer producer) {
        // persist state
        client.addScheduleState(state);
        // TODO, see ScheduleState comments on how to better store these configs
        // store policy assignment
        // store monitored stream

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

    /**
     * shutdown background threads and release various resources
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
        private final static Logger LOG = LoggerFactory.getLogger(PolicyChangeHandler.class);
        private Config config;
        private IMetadataServiceClient client;

        public PolicyChangeHandler(Config config) {
            this.config = config;
            this.client = new MetadataServiceClientImpl(config);
        }

        @Override
        public void onPolicyChange(List<PolicyDefinition> allPolicies, Collection<String> addedPolicies,
                Collection<String> removedPolicies, Collection<String> modifiedPolicies) {
            LOG.info("policy changed ... ");
            LOG.info("allPolicies: " + allPolicies + ", addedPolicies: " + addedPolicies + ", removedPolicies: "
                    + removedPolicies + ", modifiedPolicies: " + modifiedPolicies);

            IScheduleContext context = new ScheduleContextBuilder(client).buildContext();
            TopologyMgmtService mgmtService = new TopologyMgmtService();
            IPolicyScheduler scheduler = PolicySchedulerFactory.createScheduler();

            scheduler.init(context, mgmtService);

            ScheduleState state = scheduler.schedule(new ScheduleOption());

            ConfigBusProducer producer = new ConfigBusProducer(ZKConfigBuilder.getZKConfig(config));
            postSchedule(client, state, producer);
            producer.send("spout", new ConfigValue());
        }
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.load().getConfig(COORDINATOR);
        // build dynamic policy loader
        String host = config.getString(METADATA_SERVICE_HOST);
        int port = config.getInt(METADATA_SERVICE_PORT);
        String context = config.getString(METADATA_SERVICE_CONTEXT);
        IMetadataServiceClient client = new MetadataServiceClientImpl(host, port, context);
        DynamicPolicyLoader loader = new DynamicPolicyLoader(client);
        loader.addPolicyChangeListener(new PolicyChangeHandler(config));

        // schedule dynamic policy loader
        long initDelayMillis = config.getLong(DYNAMIC_POLICY_LOADER_INIT_MILLS);
        long delayMillis = config.getLong(DYNAMIC_POLICY_LOADER_DELAY_MILLS);
        ScheduledExecutorService scheduleSrv = Executors.newScheduledThreadPool(1);
        scheduleSrv.scheduleAtFixedRate(loader, initDelayMillis, delayMillis, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(new CoordinatorShutdownHook(scheduleSrv)));
        LOG.info("Eagle Coordinator started ...");
        
        Thread.currentThread().join();
    }
}
