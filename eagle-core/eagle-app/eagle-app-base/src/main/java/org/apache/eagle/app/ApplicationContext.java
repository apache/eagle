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
package org.apache.eagle.app;

import com.typesafe.config.Config;
import org.apache.eagle.alert.engine.coordinator.StreamDefinition;
import org.apache.eagle.app.sink.StreamSink;
import org.apache.eagle.app.sink.mapper.*;
import org.apache.eagle.app.spi.ApplicationProvider;
import org.apache.eagle.metadata.model.ApplicationEntity;
import org.apache.eagle.metadata.model.StreamDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Application Context Interface: org.apache.eagle.app.ApplicationContext
 *
 * <ul>
 *     <li>Application Metadata Entity (Persistence): org.apache.eagle.metadata.model.ApplicationEntity</li>
 *     <li>Application Processing Logic (Execution): org.apache.eagle.app.Application</li>
 *     <li>Application Lifecycle Listener (Installation): org.apache.eagle.app.ApplicationLifecycleListener</li>
 * </ul>
 */
public class ApplicationContext implements Serializable, ApplicationLifecycleListener{
    private final Config envConfig;
    private final ApplicationEntity appEntity;
    private final static Logger LOG = LoggerFactory.getLogger(ApplicationContext.class);
    private final Map<String, StreamDefinition> streamDefinitionMap;
    private final Map<String,StreamDesc> streamDescMap;
    private final List<ApplicationLifecycleListener> applicationLifecycleListeners;
    private final ApplicationProvider appProvider;

    /**
     * @param appEntity ApplicationEntity
     * @param appProvider ApplicationProvider
     * @param envConfig Config
     */
    public ApplicationContext(ApplicationEntity appEntity, ApplicationProvider appProvider, Config envConfig){
        this.appEntity = appEntity;
        this.envConfig = envConfig;
        this.appProvider = appProvider;
        this.streamDefinitionMap = new HashMap<>();
        this.streamDescMap = new HashMap<>();
        this.applicationLifecycleListeners = new LinkedList<>();
        doInit();
    }

    public void registerListener(ApplicationLifecycleListener listener){
        applicationLifecycleListeners.add(listener);
    }

    public List<ApplicationLifecycleListener> getListeners(){
        return applicationLifecycleListeners;
    }

    private void doInit() {
        Class<?> sinkClass = appEntity.getDescriptor().getSinkClass();
        List<StreamDefinition> outputStreams = appEntity.getDescriptor().getStreams();
        if(null != outputStreams){
            outputStreams.forEach((stream) -> {
                try {
                    StreamDesc streamDesc = new StreamDesc();
                    StreamSink streamSink = (StreamSink) sinkClass.newInstance();
                    streamDesc.setSink(streamSink.init(stream,this));
                    streamDesc.setSchema(stream);
                    streamDesc.setStreamId(stream.getStreamId());
                    streamDescMap.put(streamDesc.getStreamId(),streamDesc);
                    streamDefinitionMap.put(streamDesc.getStreamId(),stream);
                    registerListener(streamSink);
                } catch (InstantiationException | IllegalAccessException e) {
                    LOG.error("Failed to initialize instance "+sinkClass.getCanonicalName()+" for application: {}",this.getAppEntity());
                    throw new RuntimeException("Failed to initialize instance "+sinkClass.getCanonicalName()+" for application:"+this.getAppEntity(),e);
                }
            });
        }
    }

    public ApplicationEntity getAppEntity() {
        return appEntity;
    }

    public Config getEnvConfig() {
        return envConfig;
    }

    /**
     * Make sure streamId is declared in Application Providers
     *
     * @param streamId
     * @return
     */
    public StreamSink getFlattenStreamSink(String streamId, StreamEventMapper mapper){
        checkStreamExists(streamId);
        Class<?> sinkClass = appEntity.getDescriptor().getSinkClass();
        try {
            StreamSink abstractStreamSink = (StreamSink) sinkClass.newInstance();
            abstractStreamSink.setEventMapper(mapper);
            abstractStreamSink.init(streamDefinitionMap.get(streamId),this);
            return abstractStreamSink;
        } catch (InstantiationException | IllegalAccessException e) {
            LOG.error("Failed to instantiate "+sinkClass,e);
            throw new IllegalStateException("Failed to instantiate "+sinkClass,e);
        }
    }

    /**
     * Make sure streamId is declared in Application Providers
     *
     * @param streamId
     * @return
     */
    public StreamSink getDirectStreamSink(String streamId, String ... fieldNames){
        return getFlattenStreamSink(streamId,new FieldNameDirectEventMapper(fieldNames));
    }

    /**
     * Make sure streamId is declared in Application Providers
     *
     * @param streamId
     * @return
     */
    public StreamSink getDirectStreamSink(String streamId, int ... fieldIndexs){
        return getFlattenStreamSink(streamId,new FieldIndexDirectEventMapper(fieldIndexs));
    }

    /**
     * Make sure streamId is declared in Application Providers
     *
     * @param streamId
     * @return
     */
    public StreamSink getFlattenStreamSink(String streamId, TimestampSelector timestampSelector){
        checkStreamExists(streamId);
        return getFlattenStreamSink(streamId,new FlattenEventMapper(streamDefinitionMap.get(streamId),timestampSelector));
    }

    /**
     * Make sure streamId is declared in Application Providers
     *
     * @param streamId
     * @return
     */
    public StreamSink getFlattenStreamSink(String streamId, String timestampField){
        checkStreamExists(streamId);
        return getFlattenStreamSink(streamId,new FlattenEventMapper(streamDefinitionMap.get(streamId),timestampField));
    }

    /**
     * Make sure streamId is declared in Application Providers
     *
     * @param streamId
     * @return
     */
    public StreamSink getFlattenStreamSink(String streamId){
        checkStreamExists(streamId);
        return getFlattenStreamSink(streamId,new FlattenEventMapper(streamDefinitionMap.get(streamId)));
    }

    private void checkStreamExists(String streamId){
        if(! streamDefinitionMap.containsKey(streamId)){
            LOG.error("Stream [streamId = "+streamId+"] is not defined in "
                    + appEntity.getDescriptor().getProviderClass().getCanonicalName());
            throw new IllegalStateException("Stream [streamId = "+streamId+"] is not defined in "
                    + appEntity.getDescriptor().getProviderClass().getCanonicalName());
        }
    }

    public Collection<StreamDesc> getStreamSinkDescs(){
        return streamDescMap.values();
    }

    @Override
    public void onAppInstall() {
        getListeners().forEach((listener)->{
            try {
                listener.onAppInstall();
            }catch (Throwable throwable){
                LOG.error("Failed to invoked onAppInstall of listener {}",listener.toString(),throwable);
                throw throwable;
            }
        });
    }

    @Override
    public void onAppUninstall() {
        getListeners().forEach((listener)->{
            try {
                listener.onAppUninstall();
            }catch (Throwable throwable){
                LOG.error("Failed to invoked onAppUninstall of listener {}",listener.toString(),throwable);
                throw throwable;
            }
        });
    }

    @Override
    public void onAppStart() {
        getListeners().forEach((listener)->{
            try {
                listener.onAppStart();
            }catch (Throwable throwable){
                LOG.error("Failed to invoked onAppStart of listener {}",listener.toString(),throwable);
                throw throwable;
            }
        });
        appProvider.getApplication().start(this);
    }

    @Override
    public void onAppStop() {
        appProvider.getApplication().stop(this);
        getListeners().forEach((listener)->{
            try {
                listener.onAppStop();
            }catch (Throwable throwable){
                LOG.error("Failed to invoked onAppStop of listener {}",listener.toString(),throwable);
                throw throwable;
            }
        });
    }
}