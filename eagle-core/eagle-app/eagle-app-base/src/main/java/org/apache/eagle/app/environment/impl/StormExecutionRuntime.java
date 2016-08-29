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
import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import com.google.common.base.Preconditions;
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

public class StormExecutionRuntime implements ExecutionRuntime<StormEnvironment,StormTopology> {
    private final static Logger LOG = LoggerFactory.getLogger(StormExecutionRuntime.class);
    private static LocalCluster _localCluster;

    private StormEnvironment environment;

    private static LocalCluster getLocalCluster(){
        if(_localCluster == null){
            _localCluster = new LocalCluster();
        }
        return _localCluster;
    }

    @Override
    public void prepare(StormEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public StormEnvironment environment() {
        return this.environment;
    }

    private final static String STORM_NIMBUS_HOST_CONF_PATH = "application.storm.nimbusHost";
    private final static String STORM_NIMBUS_HOST_DEFAULT = "localhost";
    private final static Integer STORM_NIMBUS_THRIFT_DEFAULT = 6627;
    private final static String STORM_NIMBUS_THRIFT_CONF_PATH = "application.storm.nimbusThriftPort";
    private static final String WORKERS = "workers";

    public backtype.storm.Config getStormConfig(){
        backtype.storm.Config conf = new backtype.storm.Config();
        conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, Int.box(64 * 1024));
        conf.put(backtype.storm.Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, Int.box(8));
        conf.put(backtype.storm.Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, Int.box(32));
        conf.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, Int.box(16384));
        conf.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, Int.box(16384));
        conf.put(backtype.storm.Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, Int.box(20480000));
        String nimbusHost = STORM_NIMBUS_HOST_DEFAULT;
        if(environment.config().hasPath(STORM_NIMBUS_HOST_CONF_PATH)) {
            nimbusHost = environment.config().getString(STORM_NIMBUS_HOST_CONF_PATH);
            LOG.info("Overriding {} = {}",STORM_NIMBUS_HOST_CONF_PATH,nimbusHost);
        } else {
            LOG.info("Using default {} = {}",STORM_NIMBUS_HOST_CONF_PATH,STORM_NIMBUS_HOST_DEFAULT);
        }
        Integer nimbusThriftPort =  STORM_NIMBUS_THRIFT_DEFAULT;
        if(environment.config().hasPath(STORM_NIMBUS_THRIFT_CONF_PATH)) {
            nimbusThriftPort = environment.config().getInt(STORM_NIMBUS_THRIFT_CONF_PATH);
            LOG.info("Overriding {} = {}",STORM_NIMBUS_THRIFT_CONF_PATH,nimbusThriftPort);
        } else {
            LOG.info("Using default {} = {}",STORM_NIMBUS_THRIFT_CONF_PATH,STORM_NIMBUS_THRIFT_DEFAULT);
        }
        conf.put(backtype.storm.Config.NIMBUS_HOST, nimbusHost);
        conf.put(backtype.storm.Config.NIMBUS_THRIFT_PORT, nimbusThriftPort);
        conf.put(Config.STORM_THRIFT_TRANSPORT_PLUGIN, "backtype.storm.security.auth.SimpleTransportPlugin");
        if (environment.config().hasPath(WORKERS)) {
            conf.setNumWorkers(environment.config().getInt(WORKERS));
        }
        return conf;
    }

    @Override
    public void start(Application<StormEnvironment, StormTopology> executor, com.typesafe.config.Config config){
        String topologyName = config.getString("appId");
        Preconditions.checkNotNull(topologyName,"[appId] is required by null for "+executor.getClass().getCanonicalName());
        StormTopology topology = executor.execute(config, environment);
        LOG.info("Starting {} ({})",topologyName,executor.getClass().getCanonicalName());
        Config conf = getStormConfig();
        if(config.getString("mode").equals(ApplicationEntity.Mode.CLUSTER.name())){
            String jarFile = config.hasPath("jarPath") ? config.getString("jarPath") : null;
            if(jarFile == null){
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
        LOG.info("Stopping topology {} ..." + appId);
        if(ApplicationEntity.Mode.CLUSTER.name().equals(config.getString("mode"))){
            Nimbus.Client stormClient = NimbusClient.getConfiguredClient(getStormConfig()).getClient();
            try {
                stormClient.killTopology(appId);
            } catch (NotAliveException | TException e) {
                LOG.error("Failed to kill topology named {}, due to: {}",appId,e.getMessage(),e.getCause());
            }
        } else {
            KillOptions killOptions = new KillOptions();
            killOptions.set_wait_secs(0);
            getLocalCluster().killTopologyWithOpts(appId,killOptions);
        }
        LOG.info("Stopped topology {} ..." + appId);
    }

    @Override
    public void status(Application<StormEnvironment, StormTopology> executor, com.typesafe.config.Config config) {
        // TODO: Not implemented yet!
        throw new RuntimeException("TODO: Not implemented yet!");
    }

    public static class Provider implements ExecutionRuntimeProvider<StormEnvironment,StormTopology> {
        @Override
        public StormExecutionRuntime get() {
            return new StormExecutionRuntime();
        }
    }
}
