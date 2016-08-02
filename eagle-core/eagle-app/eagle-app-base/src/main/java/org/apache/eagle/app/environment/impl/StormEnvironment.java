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
import org.apache.eagle.app.environment.AbstractEnvironment;
import org.apache.eagle.app.sink.LoggingStreamSinkBolt;
import org.apache.eagle.app.sink.StreamSinkBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;
import storm.trident.spout.RichSpoutBatchExecutor;

/**
 * Storm Execution Environment Context
 */
public class StormEnvironment extends AbstractEnvironment {
    private final Config config;
    private final static Logger LOG = LoggerFactory.getLogger(StormEnvironment.class);
    public StormEnvironment(Config envConfig) {
        this.config = envConfig;
    }

    public StreamSinkBolt getStreamSink(String streamId) {
        return new LoggingStreamSinkBolt();
    }

    @Override
    public Config getConfig() {
        return config;
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

        if(config.hasPath(STORM_NIMBUS_HOST_CONF_PATH)) {
            nimbusHost = config.getString(STORM_NIMBUS_HOST_CONF_PATH);
            LOG.info("Overriding {} = {}",STORM_NIMBUS_HOST_CONF_PATH,nimbusHost);
        } else {
            LOG.info("Using default {} = {}",STORM_NIMBUS_HOST_CONF_PATH,STORM_NIMBUS_HOST_DEFAULT);
        }
        Integer nimbusThriftPort =  STORM_NIMBUS_THRIFT_DEFAULT;
        if(config.hasPath(STORM_NIMBUS_THRIFT_CONF_PATH)) {
            nimbusThriftPort = config.getInt(STORM_NIMBUS_THRIFT_CONF_PATH);
            LOG.info("Overriding {} = {}",STORM_NIMBUS_THRIFT_CONF_PATH,nimbusThriftPort);
        } else {
            LOG.info("Using default {} = {}",STORM_NIMBUS_THRIFT_CONF_PATH,STORM_NIMBUS_THRIFT_DEFAULT);
        }
        conf.put(backtype.storm.Config.NIMBUS_HOST, nimbusHost);
        conf.put(backtype.storm.Config.NIMBUS_THRIFT_PORT, nimbusThriftPort);
        return conf;
    }

//    /**
//     * Make sure streamId is declared in Application Providers
//     *
//     * @param streamId
//     * @return
//     */
//    public StreamSinkBolt getFlattenStreamSink(String streamId, StreamEventMapper mapper){
//        checkStreamExists(streamId);
//        Class<?> sinkClass = entity.getDescriptor().getSinkClass();
//        try {
//            StreamSinkBolt abstractStreamSink = (StreamSinkBolt) sinkClass.newInstance();
//            abstractStreamSink.setEventMapper(mapper);
//            abstractStreamSink.init(streamDefinitionMap.get(streamId),this);
//            return abstractStreamSink;
//        } catch (InstantiationException | IllegalAccessException e) {
//            LOG.error("Failed to instantiate "+sinkClass,e);
//            throw new IllegalStateException("Failed to instantiate "+sinkClass,e);
//        }
//    }
//
//    /**
//     * Make sure streamId is declared in Application Providers
//     *
//     * @param streamId
//     * @return
//     */
//    public StreamSinkBolt getDirectStreamSink(String streamId, String ... fieldNames){
//        return getFlattenStreamSink(streamId,new FieldNameDirectEventMapper(fieldNames));
//    }
//
//    /**
//     * Make sure streamId is declared in Application Providers
//     *
//     * @param streamId
//     * @return
//     */
//    public StreamSinkBolt getDirectStreamSink(String streamId, int ... fieldIndexs){
//        return getFlattenStreamSink(streamId,new FieldIndexDirectEventMapper(fieldIndexs));
//    }
//
//    /**
//     * Make sure streamId is declared in Application Providers
//     *
//     * @param streamId
//     * @return
//     */
//    public StreamSinkBolt getFlattenStreamSink(String streamId, TimestampSelector timestampSelector){
//        checkStreamExists(streamId);
//        return getFlattenStreamSink(streamId,new FlattenEventMapper(streamDefinitionMap.get(streamId),timestampSelector));
//    }
//
//    /**
//     * Make sure streamId is declared in Application Providers
//     *
//     * @param streamId
//     * @return
//     */
//    public StreamSinkBolt getFlattenStreamSink(String streamId, String timestampField){
//        checkStreamExists(streamId);
//        return getFlattenStreamSink(streamId,new FlattenEventMapper(streamDefinitionMap.get(streamId),timestampField));
//    }
//
//    /**
//     * Make sure streamId is declared in Application Providers
//     *
//     * @param streamId
//     * @return
//     */
//    public StreamSinkBolt getFlattenStreamSink(String streamId){
//        checkStreamExists(streamId);
//        return getFlattenStreamSink(streamId,new FlattenEventMapper(streamDefinitionMap.get(streamId)));
//    }
//
//    private void checkStreamExists(String streamId){
//        if(! streamDefinitionMap.containsKey(streamId)){
//            LOG.error("Stream [streamId = "+streamId+"] is not defined in "
//                    + entity.getDescriptor().getProviderClass().getCanonicalName());
//            throw new IllegalStateException("Stream [streamId = "+streamId+"] is not defined in "
//                    + entity.getDescriptor().getProviderClass().getCanonicalName());
//        }
//    }
//
//    public Collection<StreamDesc> getStreamSinkDescs(){
//        return streamDescMap.values();
//    }
//
//    public void registerListener(ApplicationLifecycle listener){
//        applicationLifecycleListeners.add(listener);
//    }
//
//    public List<ApplicationLifecycle> getListeners(){
//        return applicationLifecycleListeners;
//    }
//
//    private void doInit() {
//        Class<?> sinkClass = entity.getDescriptor().getSinkClass();
//        List<StreamDefinition> outputStreams = entity.getDescriptor().getStreams();
//        if(null != outputStreams){
//            outputStreams.forEach((stream) -> {
//                try {
//                    StreamDesc streamDesc = new StreamDesc();
//                    StreamSinkBolt streamSink = (StreamSinkBolt) sinkClass.newInstance();
//                    streamDesc.setSink(streamSink.init(stream,this));
//                    streamDesc.setSchema(stream);
//                    streamDesc.setStreamId(stream.getStreamId());
//                    streamDescMap.put(streamDesc.getStreamId(),streamDesc);
//                    streamDefinitionMap.put(streamDesc.getStreamId(),stream);
//                    registerListener(streamSink);
//                } catch (InstantiationException | IllegalAccessException e) {
//                    LOG.error("Failed to initialize instance "+sinkClass.getCanonicalName()+" for application: {}",this.entity);
//                    throw new RuntimeException("Failed to initialize instance "+sinkClass.getCanonicalName()+" for application:"+this.entity,e);
//                }
//            });
//        }
//    }
}