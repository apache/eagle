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

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import com.google.common.base.Preconditions;
import com.typesafe.config.ConfigRenderOptions;
import org.apache.eagle.alert.engine.runner.StormMetricTaggedConsumer;
import org.apache.eagle.alert.metric.MetricConfigs;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeProvider;
import org.apache.eagle.app.utils.DynamicJarPathFinder;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;
import storm.trident.spout.RichSpoutBatchExecutor;

import java.util.List;
import java.util.Objects;

public class StormExecutionRuntime implements ExecutionRuntime<StormEnvironment,StormTopology> {
    private static final Logger LOG = LoggerFactory.getLogger(StormExecutionRuntime.class);
    private static LocalCluster _localCluster;

    private StormEnvironment environment;
    private KillOptions killOptions;

    private static LocalCluster getLocalCluster() {
        if (_localCluster == null) {
            _localCluster = new LocalCluster();
        }
        return _localCluster;
    }

    public StormExecutionRuntime() {
        this.killOptions = new KillOptions();
        killOptions.set_wait_secs(0);
    }

    @Override
    public void prepare(StormEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public StormEnvironment environment() {
        return this.environment;
    }

    public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";

    private static final String STORM_NIMBUS_HOST_CONF_PATH = "application.storm.nimbusHost";
    private static final String STORM_NIMBUS_HOST_DEFAULT = "localhost";
    private static final Integer STORM_NIMBUS_THRIFT_DEFAULT = 6627;
    private static final String STORM_NIMBUS_THRIFT_CONF_PATH = "application.storm.nimbusThriftPort";

    private static final String WORKERS = "workers";

    private backtype.storm.Config getStormConfig(com.typesafe.config.Config config) {
        backtype.storm.Config conf = new backtype.storm.Config();
        conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, Int.box(64 * 1024));
        conf.put(backtype.storm.Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, Int.box(8));
        conf.put(backtype.storm.Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, Int.box(32));
        conf.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, Int.box(16384));
        conf.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, Int.box(16384));
        conf.put(backtype.storm.Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, Int.box(20480000));
        String nimbusHost = STORM_NIMBUS_HOST_DEFAULT;
        if (environment.config().hasPath(STORM_NIMBUS_HOST_CONF_PATH)) {
            nimbusHost = environment.config().getString(STORM_NIMBUS_HOST_CONF_PATH);
            LOG.info("Overriding {} = {}",STORM_NIMBUS_HOST_CONF_PATH,nimbusHost);
        } else {
            LOG.info("Using default {} = {}",STORM_NIMBUS_HOST_CONF_PATH,STORM_NIMBUS_HOST_DEFAULT);
        }
        Integer nimbusThriftPort =  STORM_NIMBUS_THRIFT_DEFAULT;
        if (environment.config().hasPath(STORM_NIMBUS_THRIFT_CONF_PATH)) {
            nimbusThriftPort = environment.config().getInt(STORM_NIMBUS_THRIFT_CONF_PATH);
            LOG.info("Overriding {} = {}",STORM_NIMBUS_THRIFT_CONF_PATH,nimbusThriftPort);
        } else {
            LOG.info("Using default {} = {}",STORM_NIMBUS_THRIFT_CONF_PATH,STORM_NIMBUS_THRIFT_DEFAULT);
        }
        conf.put(backtype.storm.Config.NIMBUS_HOST, nimbusHost);
        conf.put(backtype.storm.Config.NIMBUS_THRIFT_PORT, nimbusThriftPort);
        conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, "backtype.storm.security.auth.SimpleTransportPlugin");
        if (config.hasPath(WORKERS)) {
            conf.setNumWorkers(config.getInt(WORKERS));
        }

        if (config.hasPath(TOPOLOGY_MESSAGE_TIMEOUT_SECS)) {
            conf.put(TOPOLOGY_MESSAGE_TIMEOUT_SECS, config.getInt(TOPOLOGY_MESSAGE_TIMEOUT_SECS));
        }

        if (config.hasPath(MetricConfigs.METRIC_SINK_CONF)) {
            conf.registerMetricsConsumer(StormMetricTaggedConsumer.class, config.root().render(ConfigRenderOptions.concise()), 1);
        }
        return conf;
    }

    @Override
    public void start(Application<StormEnvironment, StormTopology> executor, com.typesafe.config.Config config) {
        String topologyName = config.getString("appId");
        Preconditions.checkNotNull(topologyName,"[appId] is required by null for " + executor.getClass().getCanonicalName());
        StormTopology topology = executor.execute(config, environment);
        LOG.info("Starting {} ({}), mode: {}",topologyName, executor.getClass().getCanonicalName(), config.getString("mode"));
        Config conf = getStormConfig(config);
        if (ApplicationEntity.Mode.CLUSTER.name().equalsIgnoreCase(config.getString("mode"))) {
            String jarFile = config.hasPath("jarPath") ? config.getString("jarPath") : null;
            if (jarFile == null) {
                jarFile = DynamicJarPathFinder.findPath(executor.getClass());
            }
            synchronized (StormExecutionRuntime.class) {
                System.setProperty("storm.jar", jarFile);
                LOG.info("Submitting as cluster mode ...");
                try {
                    StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology);
                } catch (AlreadyAliveException | InvalidTopologyException e) {
                    LOG.error(e.getMessage(), e);
                    throw new RuntimeException(e.getMessage(),e);
                } finally {
                    System.clearProperty("storm.jar");
                }
            }
        } else {
            LOG.info("Submitting as local mode ...");
            getLocalCluster().submitTopology(topologyName, conf, topology);
            LOG.info("Submitted");
        }
        LOG.info("Started {} ({})",topologyName,executor.getClass().getCanonicalName());
    }

    @Override
    public void stop(Application<StormEnvironment, StormTopology> executor, com.typesafe.config.Config config) {
        String appId = config.getString("appId");
        LOG.info("Stopping topology {} ...", appId);
        if (Objects.equals(config.getString("mode"), ApplicationEntity.Mode.CLUSTER.name())) {
            Nimbus.Client stormClient = NimbusClient.getConfiguredClient(getStormConfig(config)).getClient();
            try {
                stormClient.killTopologyWithOpts(appId, this.killOptions);
            } catch (NotAliveException | TException e) {
                LOG.error("Failed to kill topology named {}, due to: {}",appId,e.getMessage(),e.getCause());
                throw new RuntimeException(e.getMessage(),e);
            }
        } else {
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            getLocalCluster().killTopologyWithOpts(appId,killOptions);
        }
        LOG.info("Stopped topology {}", appId);
    }

    @Override
    public ApplicationEntity.Status status(Application<StormEnvironment, StormTopology> executor, com.typesafe.config.Config config) {
        String appId = config.getString("appId");
        LOG.info("Fetching status of topology {} ...", appId);
        List<TopologySummary> topologySummaries ;
        ApplicationEntity.Status status = null;
        try {
            if (Objects.equals(config.getString("mode"), ApplicationEntity.Mode.CLUSTER.name())) {
                Nimbus.Client stormClient = NimbusClient.getConfiguredClient(getStormConfig(config)).getClient();
                topologySummaries = stormClient.getClusterInfo().get_topologies();
            } else {
                topologySummaries = getLocalCluster().getClusterInfo().get_topologies();
            }

            for (TopologySummary topologySummary : topologySummaries) {
                if (topologySummary.get_name().equalsIgnoreCase(appId)) {
                    if (topologySummary.get_status().equalsIgnoreCase("ACTIVE")) {
                        status = ApplicationEntity.Status.RUNNING;
                    } else if (topologySummary.get_status().equalsIgnoreCase("INACTIVE")) {
                        status = ApplicationEntity.Status.STOPPED;
                    } else if (topologySummary.get_status().equalsIgnoreCase("KILLED")) {
                        status = ApplicationEntity.Status.REMOVED;
                    }
                }
            }
            //If not exist, return removed
            if (status == null) {
                status = ApplicationEntity.Status.REMOVED;
            }
        } catch (TException e) {
            LOG.error("Got error to fetch status of {}", appId, e);
            status = ApplicationEntity.Status.UNKNOWN;
        }
        LOG.info("Status of {}: {}", appId, status);
        return status;
    }

    public static class Provider implements ExecutionRuntimeProvider<StormEnvironment,StormTopology> {
        @Override
        public StormExecutionRuntime get() {
            return new StormExecutionRuntime();
        }
    }
}
