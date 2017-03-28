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

import com.typesafe.config.Config;
import org.apache.eagle.app.Application;
import org.apache.eagle.app.environment.ExecutionRuntime;
import org.apache.eagle.app.environment.ExecutionRuntimeProvider;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SparkExecutionRuntime implements ExecutionRuntime<SparkEnvironment, JavaStreamingContext> {


    private static final Logger LOG = LoggerFactory.getLogger(SparkExecutionRuntime.class);
    private static final String TOPOLOGY_MAINCLASS = "topology.mainclass";
    private static final String TOPOLOGY_SPARKHOME = "topology.sparkhome";
    private static final String TOPOLOGY_VERBOSE = "topology.verbose";
    private static final String TOPOLOGY_SPARKUIPORT = "topology.sparkuiport";
    private static final String TOPOLOGY_APPRESOURCE = "topology.appresource";
    private static final String TOPOLOGY_YARNQUEUE = "topology.yarnqueue";
    private static final String TOPOLOGY_SPARKCONFFILEPATH = "topology.sparkconffilepath";
    private SparkEnvironment environment;
    private static final long TIMEOUT = 120;
    private static final SparkExecutionRuntime INSTANCE = new SparkExecutionRuntime();
    private SparkAppHandle appHandle;
    private static final String SPARK_EXECUTOR_CORES = "topology.core";
    private static final String SPARK_EXECUTOR_MEMORY = "topology.memory";
    private static final String TOPOLOGY_MASTER = "topology.master";
    private static final String DRIVER_MEMORY = "topology.driverMemory";
    private static final String DRIVER_CORES = "topology.driverCores";
    private static final String DEPLOY_MODE = "topology.deployMode";
    private static final String TOPOLOGY_NAME = "topology.name";
    private static final String TOPOLOGY_DYNAMICALLOCATION = "topology.dynamicAllocation";
    private static final String BATCH_DURATION = "topology.batchDuration";
    private static final String ROUTER_TASK_NUM = "topology.numOfRouterBolts";
    private static final String ALERT_TASK_NUM = "topology.numOfAlertBolts";
    private static final String PUBLISH_TASK_NUM = "topology.numOfPublishTasks";
    private static final String SLIDE_DURATION_SECOND = "topology.slideDurations";
    private static final String WINDOW_DURATIONS_SECOND = "topology.windowDurations";
    private static final String CHECKPOINT_PATH = "topology.checkpointPath";
    private static final String TOPOLOGY_GROUPID = "topology.groupId";
    private static final String AUTO_OFFSET_RESET = "topology.offsetreset";
    private static final String EAGLE_CORRELATION_CONTEXT = "metadataService.context";
    private static final String EAGLE_CORRELATION_SERVICE_PORT = "metadataService.port";
    private static final String EAGLE_CORRELATION_SERVICE_HOST = "metadataService.host";
    private static final String TOPOLOGY_MULTIKAFKA = "topology.multikafka";
    private static final String SPOUT_KAFKABROKERZKQUORUM = "spout.kafkaBrokerZkQuorum";
    private static final String ZKCONFIG_ZKQUORUM = "zkConfig.zkQuorum";

    @Override
    public void prepare(SparkEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public SparkEnvironment environment() {
        return this.environment;
    }


    private SparkLauncher prepareSparkConfig(Config config) {
        String master = config.hasPath(TOPOLOGY_MASTER) ? config.getString(TOPOLOGY_MASTER) : "local[*]";
        String sparkExecutorCores = config.getString(SPARK_EXECUTOR_CORES);
        String sparkExecutorMemory = config.getString(SPARK_EXECUTOR_MEMORY);
        String driverMemory = config.getString(DRIVER_MEMORY);
        String driverCore = config.getString(DRIVER_CORES);
        String deployMode = config.getString(DEPLOY_MODE);
        String enable = config.getString(TOPOLOGY_DYNAMICALLOCATION);
        boolean verbose = config.getBoolean(TOPOLOGY_VERBOSE);
        String mainClass = config.getString(TOPOLOGY_MAINCLASS);
        String sparkHome = config.getString(TOPOLOGY_SPARKHOME);
        String uiport = config.getString(TOPOLOGY_SPARKUIPORT);
        String appResource = config.getString(TOPOLOGY_APPRESOURCE);
        String yarnqueue = config.getString(TOPOLOGY_YARNQUEUE);


        SparkLauncher sparkLauncher = new SparkLauncher();
        sparkLauncher.setMaster(master);
        sparkLauncher.setMainClass(mainClass);
        sparkLauncher.setSparkHome(sparkHome);
        //sparkLauncher.setJavaHome(TOPOLOGY_JAVAHOME);
        sparkLauncher.setDeployMode(deployMode);
        sparkLauncher.setVerbose(verbose);
        sparkLauncher.setAppResource(appResource);
        sparkLauncher.setAppName(config.getString(TOPOLOGY_NAME));
        sparkLauncher.setConf("spark.yarn.queue", yarnqueue);
        sparkLauncher.setConf("spark.executor.cores", sparkExecutorCores);
        sparkLauncher.setConf("spark.executor.memory", sparkExecutorMemory);
        sparkLauncher.setConf("spark.driver.memory", driverMemory);
        sparkLauncher.setConf("spark.driver.cores", driverCore);
        sparkLauncher.setConf("spark.streaming.dynamicAllocation.enable", enable);
        sparkLauncher.setConf("spark.ui.port", uiport);
        String path = config.getString(TOPOLOGY_SPARKCONFFILEPATH);
        if (StringUtil.isNotBlank(path)) {
            sparkLauncher.setPropertiesFile(path);
        }

        String batchDuration = config.getString(BATCH_DURATION);
        String routerTasknum = config.getString(ROUTER_TASK_NUM);
        String alertTasknum = config.getString(ALERT_TASK_NUM);
        String publishTasknum = config.getString(PUBLISH_TASK_NUM);
        String slideDurationsecond = config.getString(SLIDE_DURATION_SECOND);
        String windowDurationssecond = config.getString(WINDOW_DURATIONS_SECOND);
        String checkpointPath = config.getString(CHECKPOINT_PATH);
        String topologyGroupid = config.getString(TOPOLOGY_GROUPID);
        String autoOffsetReset = config.getString(AUTO_OFFSET_RESET);
        String restApihost = config.getString(EAGLE_CORRELATION_SERVICE_HOST);
        String restApiport = config.getString(EAGLE_CORRELATION_SERVICE_PORT);
        String restApicontext = config.getString(EAGLE_CORRELATION_CONTEXT);
        String useMultiKafka = config.getString(TOPOLOGY_MULTIKAFKA);
        String kafkaBrokerZkQuorum = config.getString(SPOUT_KAFKABROKERZKQUORUM);
        String zkConfigzkQuorum = config.getString(ZKCONFIG_ZKQUORUM);


        sparkLauncher.addAppArgs(batchDuration, routerTasknum, alertTasknum, publishTasknum,
            slideDurationsecond, windowDurationssecond, checkpointPath, topologyGroupid,
            autoOffsetReset, restApicontext, restApiport, restApihost, useMultiKafka, kafkaBrokerZkQuorum, zkConfigzkQuorum);
        return sparkLauncher;
    }

    @Override
    public void start(Application<SparkEnvironment, JavaStreamingContext> executor, Config config) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        SparkAppListener sparkAppListener = new SparkAppListener(countDownLatch);
        try {
            appHandle = prepareSparkConfig(config).startApplication(sparkAppListener);
            LOG.info("Starting Spark Streaming");
            appHandle.addListener(new SparkAppListener(countDownLatch));
            Thread sparkAppListenerThread = new Thread(sparkAppListener);
            sparkAppListenerThread.start();
            countDownLatch.await(TIMEOUT, TimeUnit.SECONDS);
            LOG.info("Spark Streaming is ended");
        } catch (InterruptedException e) {
            LOG.error("SparkExecutionRuntime countDownLatch InterruptedException", e);
        } catch (IOException e) {
            LOG.error("SparkLauncher().startApplication IOException", e);
        }

    }

    @Override
    public void stop(Application<SparkEnvironment, JavaStreamingContext> executor, Config config) {
        try {
            appHandle.stop();
        } catch (Exception e) {
            appHandle.kill();
        }
    }

    private static class SparkAppListener implements
        SparkAppHandle.Listener, Runnable {
        private static final Logger LOG = LoggerFactory.getLogger(SparkAppListener.class);
        private final CountDownLatch countDownLatch;

        SparkAppListener(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void stateChanged(SparkAppHandle handle) {
            String sparkAppId = handle.getAppId();
            SparkAppHandle.State appState = handle.getState();
            if (sparkAppId != null) {
                LOG.info("Spark job with app id: " + sparkAppId + ",State changed to: " + appState);
            } else {
                LOG.info("Spark job's state changed to: " + appState);
            }
            if (appState != null && appState.isFinal()) {
                countDownLatch.countDown();
            }
        }

        @Override
        public void infoChanged(SparkAppHandle handle) {
        }

        @Override
        public void run() {
        }
    }

    @Override
    public ApplicationEntity.Status status(Application<SparkEnvironment, JavaStreamingContext> executor, Config config) {

        if (appHandle == null) {
            return ApplicationEntity.Status.INITIALIZED;
        }
        SparkAppHandle.State state = appHandle.getState();
        LOG.info("Alert engine spark topology  status is " + state.name());
        ApplicationEntity.Status status;
        if (state.isFinal()) {
            status = ApplicationEntity.Status.STOPPED;
        } else if (state == SparkAppHandle.State.RUNNING) {
            return ApplicationEntity.Status.RUNNING;
        } else if (state == SparkAppHandle.State.CONNECTED || state == SparkAppHandle.State.SUBMITTED) {
            return ApplicationEntity.Status.INITIALIZED;
        } else {
            LOG.error("Alert engine spark topology status unknow");
            status = ApplicationEntity.Status.INITIALIZED;
        }
        return status;
    }

    public static class Provider implements ExecutionRuntimeProvider<SparkEnvironment, JavaStreamingContext> {
        @Override
        public SparkExecutionRuntime get() {
            return INSTANCE;
        }
    }
}