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
import org.apache.eagle.app.Configuration;
import org.apache.eagle.app.environment.AbstractEnvironment;
import org.apache.eagle.app.sink.FlattenEventMapper;
import org.apache.eagle.app.sink.LoggingStreamSink;
import org.apache.eagle.app.sink.StormStreamSink;
import org.apache.eagle.app.sink.StreamSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;
import storm.trident.spout.RichSpoutBatchExecutor;

/**
 * Storm Execution Environment Context
 */
public class StormEnvironment extends AbstractEnvironment {
    private final static Logger LOG = LoggerFactory.getLogger(StormEnvironment.class);
    public StormEnvironment(Config envConfig) {
        super(envConfig);
    }

    private final static String STORM_NIMBUS_HOST_CONF_PATH = "application.storm.nimbusHost";
    private final static String STORM_NIMBUS_HOST_DEFAULT = "localhost";
    private final static Integer STORM_NIMBUS_THRIFT_DEFAULT = 6627;
    private final static String STORM_NIMBUS_THRIFT_CONF_PATH = "application.storm.nimbusThriftPort";

    public backtype.storm.Config getStormConfig(){
        backtype.storm.Config conf = new backtype.storm.Config();
        conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, Int.box(64 * 1024));
        conf.put(backtype.storm.Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, Int.box(8));
        conf.put(backtype.storm.Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, Int.box(32));
        conf.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, Int.box(16384));
        conf.put(backtype.storm.Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, Int.box(16384));
        conf.put(backtype.storm.Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE, Int.box(20480000));
        String nimbusHost = STORM_NIMBUS_HOST_DEFAULT;
        if(config().hasPath(STORM_NIMBUS_HOST_CONF_PATH)) {
            nimbusHost = config().getString(STORM_NIMBUS_HOST_CONF_PATH);
            LOG.info("Overriding {} = {}",STORM_NIMBUS_HOST_CONF_PATH,nimbusHost);
        } else {
            LOG.info("Using default {} = {}",STORM_NIMBUS_HOST_CONF_PATH,STORM_NIMBUS_HOST_DEFAULT);
        }
        Integer nimbusThriftPort =  STORM_NIMBUS_THRIFT_DEFAULT;
        if(config().hasPath(STORM_NIMBUS_THRIFT_CONF_PATH)) {
            nimbusThriftPort = config().getInt(STORM_NIMBUS_THRIFT_CONF_PATH);
            LOG.info("Overriding {} = {}",STORM_NIMBUS_THRIFT_CONF_PATH,nimbusThriftPort);
        } else {
            LOG.info("Using default {} = {}",STORM_NIMBUS_THRIFT_CONF_PATH,STORM_NIMBUS_THRIFT_DEFAULT);
        }
        conf.put(backtype.storm.Config.NIMBUS_HOST, nimbusHost);
        conf.put(backtype.storm.Config.NIMBUS_THRIFT_PORT, nimbusThriftPort);
        return conf;
    }

    public StormStreamSink getFlattenStreamSink(String streamId,Configuration appConfig) {
        return ((StormStreamSink) streamSink().getSink(streamId,appConfig)).setEventMapper(new FlattenEventMapper(streamId));
    }
}