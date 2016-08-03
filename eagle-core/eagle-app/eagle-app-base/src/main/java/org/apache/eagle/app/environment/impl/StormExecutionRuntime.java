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
import org.apache.eagle.app.Configuration;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeProvider;
import org.apache.eagle.app.utils.DynamicJarPathFinder;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormExecutionRuntime implements ExecutionRuntime<StormEnvironment,StormTopology> {
    private final static Logger LOG = LoggerFactory.getLogger(StormExecutionRuntime.class);
    private static LocalCluster _localCluster;

    private StormEnvironment environment;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                if(_localCluster != null) {
                    LOG.info("Shutting down local storm cluster instance");
                    _localCluster.shutdown();
                }
            }
        });
    }

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

    @Override
    public <Conf extends Configuration> void start(Application<Conf, StormEnvironment, StormTopology> executor, Conf config){
        String topologyName = config.getAppId();
        Preconditions.checkNotNull(topologyName,"[appId] is required by null for "+executor.getClass().getCanonicalName());
        StormTopology topology = executor.execute(config, environment);
        LOG.info("Starting {} ({})",topologyName,executor.getClass().getCanonicalName());
        Config conf = this.environment.getStormConfig();
        if(config.getMode() == ApplicationEntity.Mode.CLUSTER){
            if(config.getJarPath() == null) config.setJarPath(DynamicJarPathFinder.findPath(executor.getClass()));
            String jarFile = config.getJarPath();
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
    }

    @Override
    public <Conf extends Configuration> void stop(Application<Conf,StormEnvironment, StormTopology> executor, Conf config) {
        String appId = config.getAppId();
        if(config.getMode() == ApplicationEntity.Mode.CLUSTER){
            Nimbus.Client stormClient = NimbusClient.getConfiguredClient(this.environment.getStormConfig()).getClient();
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
    }

    @Override
    public <Conf extends Configuration> void status(Application<Conf,StormEnvironment, StormTopology> executor, Conf config) {
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